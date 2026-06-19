import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.stream.{BoundedSourceQueue, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}

import java.time.Instant.now
import scala.util.Try

import spray.json._

// ---------- Client Message -> Server ----------
sealed trait ClientCommand
case class PlayerJoin(player_id: String, player_name: String, x: Double, y: Double,
                      direction: String, state: String) extends ClientCommand
case class ChatMessage(player_id: String, player_name: String, message: String) extends ClientCommand
case class PlayerMove(player_name: String, x: String, y: String,
                      direction: String, state: String) extends ClientCommand

// Error Entry Flow
case class Error(error: String) extends ClientCommand

// ---------- Server Message -> Client ----------
sealed trait ServerEvent
case class ChatBroadcast(player_id: String, player_name: String, message: String) extends ServerEvent
case class EntityState(entity_id: String, entity_type: String, player_id: String, player_name: String,
                       x: Double, y: Double, direction: String, state: String)
case class WorldState(payload: List[EntityState]) extends ServerEvent
case class PlayerJoined(player_id: String, player_name: String, x: Double, y: Double) extends ServerEvent
case class UserInfo(player_id: String, player_name: String, last_activity: Long)
case class UserList(payload: List[UserInfo]) extends ServerEvent
case class ServerError(code: String, message: String) extends ServerEvent

// ---------- Wrapper and State ----------
case class ProtocolMessage(`type`: String, payload: JsValue)
case class Player(playerId: String, playerName: String, x: Double, y: Double,
                  direction: String, state: String, lastActivity: Long)

object PlayerArea {
  case class Incoming(senderUUID: String, command: ClientCommand)
  case class RegisterConnection(senderUUID: String, privateRef: BoundedSourceQueue[ServerEvent])
}

// ---------- JSON decoding ----------
trait MessageHeaderProtocol extends DefaultJsonProtocol {
  implicit val protocolMessageFormat: RootJsonFormat[ProtocolMessage] = jsonFormat2(ProtocolMessage)
}
trait ClientCommandJSON extends DefaultJsonProtocol {
  implicit val payloadPlayerJoinFormat: RootJsonFormat[PlayerJoin]   = jsonFormat6(PlayerJoin)
  implicit val payloadPlayerMoveFormat: RootJsonFormat[PlayerMove]   = jsonFormat5(PlayerMove)
  implicit val payloadChatMessageFormat: RootJsonFormat[ChatMessage] = jsonFormat3(ChatMessage)
}

trait ServerEventJSON extends DefaultJsonProtocol {
  implicit val entityStateFormat:  RootJsonFormat[EntityState]   = jsonFormat8(EntityState)
  implicit val userInfoFormat:     RootJsonFormat[UserInfo]      = jsonFormat3(UserInfo)
  implicit val playerJoinedFormat: RootJsonFormat[PlayerJoined]  = jsonFormat4(PlayerJoined)
  implicit val chatBroadcastFormat:RootJsonFormat[ChatBroadcast] = jsonFormat3(ChatBroadcast)
  implicit val serverErrorFormat:  RootJsonFormat[ServerError]   = jsonFormat2(ServerError)
}

class PlayerArea extends Actor with ActorLogging {
  import PlayerArea._

  var players     = Map[String, Player]()
  var connections = Map[String, BoundedSourceQueue[ServerEvent]]()

  override def receive: Receive = {

    case RegisterConnection(connId, out) =>
      connections = connections + (connId -> out)
      log.info(s"connections (${connections.size}) : ${connections.keys}")

    case Incoming(connId, PlayerJoin(playerId, name, x, y, dir, st)) =>
      if (playerId == null || playerId.trim.isEmpty) {
        // player_id empty -> Originating client only
        connections.get(connId).foreach(_.offer(ServerError("INVALID_PLAYER_ID", "player_id is required")))
      } else {
        players = players + (connId -> Player(playerId, name, x, y, dir, st, now.toEpochMilli))
        log.info(s"players (${players.size}) : $players")

        // world_state -> to the newly connected client only
        val entities = players.values.map(p =>
          EntityState(p.playerId, "player", p.playerId, p.playerName, p.x, p.y, p.direction, p.state)
        ).toList
        connections.get(connId).foreach(_.offer(WorldState(entities)))

        // player_joined -> to all clients except the sender
        val joined = PlayerJoined(playerId, name, x, y)
        (connections - connId).values.foreach(_.offer(joined))

        // user_list -> to all clients
        val users = players.values.map(p => UserInfo(p.playerId, p.playerName, p.lastActivity)).toList
        connections.values.foreach(_.offer(UserList(users)))
      }

    case Incoming(connId, ChatMessage(playerId, name, message)) =>
      // Updates player_name and last_activity if the player already exists
      players.get(connId).foreach { p =>
        players = players + (connId -> p.copy(playerName = name, lastActivity = now.toEpochMilli))
      }
      // Broadcasts to ALL connected clients (including the sender)
      connections.values.foreach(_.offer(ChatBroadcast(playerId, name, message)))

    // chat_message / move: not handled yet, logged to avoid being silently discarded
    case Incoming(connId, other) =>
      log.info(s"Command not implemented yet $connId : $other")
  }
}

object AkkaWebSocketServer extends App
  with MessageHeaderProtocol
  with ClientCommandJSON
  with ServerEventJSON
  with SprayJsonSupport {

  implicit val system: ActorSystem = ActorSystem("AkkaWebSocketServer")
  import system.dispatcher
  import PlayerArea._

  val gameAreaMapActor = system.actorOf(Props[PlayerArea](), "GameAreaMap")

  def encode(event: ServerEvent): String = {
    val (t, payload): (String, JsValue) = event match {
      case e: WorldState    => ("world_state",   e.payload.toJson)
      case e: UserList      => ("user_list",     e.payload.toJson)
      case e: PlayerJoined  => ("player_joined", e.toJson)
      case e: ChatBroadcast => ("chat_message",  e.toJson)
      case e: ServerError   => ("error",         e.toJson)
    }
    JsObject("type" -> JsString(t), "payload" -> payload).compactPrint
  }

  private def chatFlow(): Flow[Message, Message, NotUsed] = {
    // transport identity for THIS connection (generated by the server)
    val senderUUID = java.util.UUID.randomUUID.toString

    // private outbound channel dedicated to this connection (carries ServerEvent messages)
    val (privateRef, privateSource) = Source.queue[ServerEvent](10).preMaterialize()

    // register the connection with the actor (so it can send targeted responses)
    gameAreaMapActor ! RegisterConnection(senderUUID, privateRef)

    // ----- inbound flow: JSON parsing -> typed command -> actor -----
    val incoming = Flow[Message]
      .collect { case tm: TextMessage.Strict => tm.text }
      .map { text =>
        println(s"Incoming message: $text")
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
      // Errors are converted to ServerError messages and pushed to the private queue
      // (addressed to the sender only)
      .divertTo(
        Sink.foreach[ClientCommand] {
          case Error(e) => privateRef.offer(ServerError("ERROR", e))
          case _        => ()
        },
        { case _: Error => true; case _ => false }
      )
      // Valid commands are forwarded to the actor, tagged with the senderUUID
      .to(Sink.foreach { command =>
        gameAreaMapActor ! Incoming(senderUUID, command)
      })

    // ----- outbound flow: private queue only -----
    //
    // The actor pushes both targeted and broadcast messages to this queue,
    // broadcasting by iterating over its registered connections.
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