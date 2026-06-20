import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem, Props, Timers}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.stream.{BoundedSourceQueue, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

import java.time.Instant.now
import scala.concurrent.duration._
import scala.util.Try

import spray.json._

// ---------- Client -> Server commands ----------
sealed trait ClientCommand
case class PlayerJoin(player_id: String, player_name: String, x: Double, y: Double,
                      direction: String, state: String) extends ClientCommand
case class ChatMessage(player_id: String, player_name: String, message: String) extends ClientCommand
case class PlayerMove(player_name: String, x: String, y: String,
                      direction: String, state: String) extends ClientCommand
// Internal routing sentinel: produced by the incoming flow, diverted to the private queue.
case class Error(error: String) extends ClientCommand

// ---------- Server -> Client events ----------
sealed trait ServerEvent
case class EntityState(entity_id: String, entity_type: String, player_id: String, player_name: String,
                       x: Double, y: Double, direction: String, state: String)
case class WorldState(payload: List[EntityState]) extends ServerEvent
case class PlayerJoined(player_id: String, player_name: String, x: Double, y: Double) extends ServerEvent
// last_activity is a Unix timestamp in SECONDS (float), as required by the protocol.
case class UserInfo(player_id: String, player_name: String, last_activity: Double)
case class UserList(payload: List[UserInfo]) extends ServerEvent
case class ChatBroadcast(player_id: String, player_name: String, message: String) extends ServerEvent
case class EntityMove(entity_id: String, player_id: String, player_name: String,
                      x: Double, y: Double, direction: String, state: String) extends ServerEvent
case class PlayerLeft(player_id: String, player_name: String) extends ServerEvent
case class ServerError(code: String, message: String) extends ServerEvent

// ---------- Envelope + state ----------
case class ProtocolMessage(`type`: String, payload: JsValue)
case class Player(playerId: String, playerName: String, x: Double, y: Double,
                  direction: String, state: String, lastActivity: Long) // stored as epoch millis

object PlayerArea {
  case class Incoming(senderUUID: String, command: ClientCommand)
  case class RegisterConnection(senderUUID: String, privateRef: BoundedSourceQueue[ServerEvent])
  case class ConnectionClosed(senderUUID: String) // sent when a WebSocket stream completes/fails
  case object Tick // periodic trigger for user_list
}

// ---------- JSON: decoding incoming payloads ----------
trait MessageHeaderProtocol extends DefaultJsonProtocol {
  implicit val protocolMessageFormat: RootJsonFormat[ProtocolMessage] = jsonFormat2(ProtocolMessage)
}
trait ClientCommandJSON extends DefaultJsonProtocol {
  implicit val payloadPlayerJoinFormat: RootJsonFormat[PlayerJoin]   = jsonFormat6(PlayerJoin)
  implicit val payloadPlayerMoveFormat: RootJsonFormat[PlayerMove]   = jsonFormat5(PlayerMove)
  implicit val payloadChatMessageFormat: RootJsonFormat[ChatMessage] = jsonFormat3(ChatMessage)
}

// ---------- JSON: encoding outgoing events ----------
trait ServerEventJSON extends DefaultJsonProtocol {
  implicit val entityStateFormat:   RootJsonFormat[EntityState]   = jsonFormat8(EntityState)
  implicit val userInfoFormat:      RootJsonFormat[UserInfo]      = jsonFormat3(UserInfo)
  implicit val playerJoinedFormat:  RootJsonFormat[PlayerJoined]  = jsonFormat4(PlayerJoined)
  implicit val chatBroadcastFormat: RootJsonFormat[ChatBroadcast] = jsonFormat3(ChatBroadcast)
  implicit val entityMoveFormat:    RootJsonFormat[EntityMove]    = jsonFormat7(EntityMove)
  implicit val playerLeftFormat:    RootJsonFormat[PlayerLeft]    = jsonFormat2(PlayerLeft)
  implicit val serverErrorFormat:   RootJsonFormat[ServerError]   = jsonFormat2(ServerError)
}

class PlayerArea extends Actor with ActorLogging with Timers {
  import PlayerArea._

  // connId (senderUUID) -> player state
  var players     = Map[String, Player]()
  // connId (senderUUID) -> per-connection outbound write end
  var connections = Map[String, BoundedSourceQueue[ServerEvent]]()

  // Movement validation thresholds. PLACEHOLDERS — tune to match the Python server.
  private val MaxPlayerSpeed = 50.0
  private val MoveTolerance  = 5.0

  // Start the periodic user_list broadcast (every 5 seconds).
  override def preStart(): Unit =
    timers.startTimerWithFixedDelay("user-list", Tick, 5.seconds)

  // Broadcast the current user list to everyone. Shared by the 3 triggers
  // (timer, after player_join, after disconnect).
  private def broadcastUserList(): Unit = {
    val users = players.values.map(p =>
      UserInfo(p.playerId, p.playerName, p.lastActivity / 1000.0) // millis -> seconds (float)
    ).toList
    connections.values.foreach(_.offer(UserList(users)))
  }

  override def receive: Receive = {

    case RegisterConnection(connId, out) =>
      connections = connections + (connId -> out)
      log.info(s"connections (${connections.size}) : ${connections.keys}")

    case Tick =>
      broadcastUserList()

    case Incoming(connId, PlayerJoin(playerId, name, x, y, dir, st)) =>
      if (playerId == null || playerId.trim.isEmpty) {
        // empty player_id -> targeted error to the sender only
        connections.get(connId).foreach(_.offer(ServerError("INVALID_PLAYER_ID", "player_id is required")))
      } else {
        players = players + (connId -> Player(playerId, name, x, y, dir, st, now.toEpochMilli))
        log.info(s"players (${players.size}) : $players")

        // world_state -> to the new player only
        val entities = players.values.map(p =>
          EntityState(p.playerId, "player", p.playerId, p.playerName, p.x, p.y, p.direction, p.state)
        ).toList
        connections.get(connId).foreach(_.offer(WorldState(entities)))

        // player_joined -> to everyone except the sender
        val joined = PlayerJoined(playerId, name, x, y)
        (connections - connId).values.foreach(_.offer(joined))

        // user_list -> to everyone
        broadcastUserList()
      }

    case Incoming(connId, PlayerMove(_, xStr, yStr, dir, st)) =>
      players.get(connId) match {
        case None =>
          // move sent before player_join
          connections.get(connId).foreach(_.offer(ServerError("PLAYER_NOT_JOINED", "send player_join first")))
        case Some(p) =>
          (Try(xStr.toDouble).toOption, Try(yStr.toDouble).toOption) match {
            case (Some(newX), Some(newY)) =>
              val distance = math.hypot(newX - p.x, newY - p.y)
              if (distance > MaxPlayerSpeed + MoveTolerance) {
                // refused -> resync the sender to its last known server position (sender only)
                connections.get(connId).foreach(_.offer(
                  EntityMove(p.playerId, p.playerId, p.playerName, p.x, p.y, p.direction, p.state)))
              } else {
                // accepted -> update state and broadcast to everyone
                val updated = p.copy(x = newX, y = newY, direction = dir, state = st, lastActivity = now.toEpochMilli)
                players = players + (connId -> updated)
                connections.values.foreach(_.offer(
                  EntityMove(updated.playerId, updated.playerId, updated.playerName,
                    updated.x, updated.y, updated.direction, updated.state)))
              }
            case _ =>
              // x or y is not a number
              connections.get(connId).foreach(_.offer(ServerError("INVALID_MOVE", "x and y must be numbers")))
          }
      }

    case Incoming(connId, ChatMessage(playerId, name, message)) =>
      // update player_name + last_activity if the player already exists
      players.get(connId).foreach { p =>
        players = players + (connId -> p.copy(playerName = name, lastActivity = now.toEpochMilli))
      }
      // broadcast to ALL connected clients (sender included)
      connections.values.foreach(_.offer(ChatBroadcast(playerId, name, message)))

    case ConnectionClosed(connId) =>
      // remove the player + its outbound channel from both maps
      val gone = players.get(connId)
      players     = players - connId
      connections = connections - connId
      // notify the remaining clients, then refresh the user list
      gone.foreach { p =>
        connections.values.foreach(_.offer(PlayerLeft(p.playerId, p.playerName)))
      }
      broadcastUserList()
      log.info(s"Disconnected $connId — players: ${players.size}, connections: ${connections.size}")

    // any other command: not handled, logged so nothing is silently dropped
    case Incoming(connId, other) =>
      log.info(s"Unhandled command from $connId : $other")
  }
}

object AkkaWebSocketServer extends App
  with MessageHeaderProtocol
  with ClientCommandJSON
  with ServerEventJSON {

  implicit val system: ActorSystem = ActorSystem("AkkaWebSocketServer")
  import system.dispatcher
  import PlayerArea._

  val gameAreaMapActor = system.actorOf(Props[PlayerArea](), "GameAreaMap")

  // Encode a ServerEvent into the wire JSON: { "type": ..., "payload": ... }
  def encode(event: ServerEvent): String = {
    val (t, payload): (String, JsValue) = event match {
      case e: WorldState    => ("world_state",   e.payload.toJson)
      case e: UserList      => ("user_list",     e.payload.toJson)
      case e: PlayerJoined  => ("player_joined", e.toJson)
      case e: ChatBroadcast => ("chat_message",  e.toJson)
      case e: EntityMove    => ("entity_move",   e.toJson)
      case e: PlayerLeft    => ("player_left",   e.toJson)
      case e: ServerError   => ("error",         e.toJson)
    }
    JsObject("type" -> JsString(t), "payload" -> payload).compactPrint
  }

  private def chatFlow(): Flow[Message, Message, NotUsed] = {
    // server-assigned transport identity for THIS connection
    val senderUUID = java.util.UUID.randomUUID.toString

    // per-connection private outbound channel (carries ServerEvent)
    val (privateRef, privateSource) = Source.queue[ServerEvent](10).preMaterialize()

    // register the connection so the actor can reach it (targeted sends)
    gameAreaMapActor ! RegisterConnection(senderUUID, privateRef)

    // ----- incoming : parse JSON -> typed command -> actor -----
    val incoming = Flow[Message]
      .collect { case tm: TextMessage.Strict => tm.text }
      .map { text =>
        println(s"Received: $text")
        Try(text.parseJson.convertTo[ProtocolMessage])
      }
      .map(_.toEither)
      .map {
        case Right(protocol) =>
          Try {
            protocol.`type` match {
              case "player_join"  => protocol.payload.convertTo[PlayerJoin]
              case "move"         => protocol.payload.convertTo[PlayerMove]
              case "chat_message" => protocol.payload.convertTo[ChatMessage]
              case _              => Error("UNKNOWN_MESSAGE_TYPE")
            }
          }.getOrElse(Error("INVALID_PAYLOAD"))
        case Left(_) => Error("INVALID_JSON")
      }
      // Error sentinels are converted to ServerError and pushed to the private queue (targeted)
      .divertTo(
        Sink.foreach[ClientCommand] {
          case Error(e) => privateRef.offer(ServerError("ERROR", e))
          case _        => ()
        },
        { case _: Error => true; case _ => false }
      )
      // tag each valid command with the connection id, forward to the actor,
      // and notify the actor of disconnect via the completion/failure message.
      .map(command => Incoming(senderUUID, command))
      .to(Sink.actorRef(
        gameAreaMapActor,
        ConnectionClosed(senderUUID),                 // on normal completion (client closed)
        (_: Throwable) => ConnectionClosed(senderUUID) // on failure
      ))

    // ----- outgoing : the private queue only, encoded as JSON -----
    val outgoing = privateSource
      .buffer(64, OverflowStrategy.dropHead)
      .map(event => TextMessage(encode(event)))

    Flow.fromSinkAndSourceCoupled(incoming, outgoing)
  }

  private val websocketRoute =
    (pathEndOrSingleSlash & get) {
      handleWebSocketMessages(chatFlow())
    }

  Http().newServerAt("localhost", 8081).bind(websocketRoute)
}