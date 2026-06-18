import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, MergeHub, Sink, Source}

import scala.util.Try

// step 1 - JSON
import spray.json._

sealed trait ClientCommand

case class PlayerJoin(
                       player_id: String,
                       player_name: String,
                       x: Double,
                       y: Double,
                       direction: String,
                       state: String) extends ClientCommand

case class ChatMessage(
                        player_id: String,
                        player_name: String,
                        message: String
                      ) extends ClientCommand

case class PlayerMove(
                       player_name: String,
                       x: String,
                       y: String,
                       direction: String,
                       state: String) extends ClientCommand

case class Error(
                  error: String
                ) extends ClientCommand


case class ProtocolMessage(`type`: String, payload: JsValue)

case class Player(nickname: String, uuid: String, message: String)

object PlayerArea {

  case class GetPlayerByNickName(nickname: String)

  case class AddPlayer(player: Player)

  case class RemovePlayer(player: Player)

  case object OperationSuccess
}

// step 2 - JSON
trait PlayerJsonProtocol extends DefaultJsonProtocol {
  implicit val protocolPlayerFormat = jsonFormat3(Player)
}

trait MessageHeaderProtocol extends DefaultJsonProtocol {
  implicit val protocolMessageFormat = jsonFormat2(ProtocolMessage)
}

trait ClientCommandJSON extends DefaultJsonProtocol {
  implicit val payloadPlayerJoinFormat = jsonFormat6(PlayerJoin)
  implicit val payloadPlayerMoveFormat = jsonFormat5(PlayerMove)
  implicit val payloadChatMessageFormat = jsonFormat3(ChatMessage)
}

class PlayerArea extends Actor with ActorLogging {

  import PlayerArea._

  var players = Map[String, Player]()

  override def receive: Receive = {
    case GetPlayerByNickName(nickname) =>
      log.info("Getting player by UUID")
      sender() ! players.get(nickname)

    case AddPlayer(player) =>
      log.info(s"Trying to add player $player")
      players = players + (player.uuid -> player)
      sender() ! OperationSuccess

    case RemovePlayer(player) =>
      log.info(s"Trying to remove player $player")
      players = players - player.nickname
      sender() ! OperationSuccess
  }
}

object AkkaWebSocketServer extends App
  // step 3 - JSON
  with PlayerJsonProtocol
  with MessageHeaderProtocol
  with ClientCommandJSON
  // step 4 - JSON
  with SprayJsonSupport {
  implicit val system: ActorSystem = ActorSystem("AkkaWebSocketServer")

  import system.dispatcher
  import PlayerArea._

  val gameAreaMapActor = system.actorOf(Props[PlayerArea], "GameAreaMap")

  private val mergeHub = MergeHub.source[ClientCommand]
  private val broadcast = BroadcastHub.sink[ClientCommand]
  private val (publisherPort, subscriberPort) = mergeHub.toMat(broadcast)(Keep.both).run()

  var playersList = List().empty

  private def chatFlow(): Flow[Message, Message, NotUsed] = {
    // 1 - Generate random UUID for the player
    val senderUUID = java.util.UUID.randomUUID.toString

    // 2 - Create private queue, private for each users.
    val (privateRef, privateSource) = Source.queue[ClientCommand](10).preMaterialize()

    // 3 - Incoming flow
    val incoming = Flow[Message]
      .collect { case tm: TextMessage.Strict => tm.text }
      .map { text => println(s"Recu: $text")
        Try {
          text.parseJson.convertTo[ProtocolMessage]
        }
      }
      .map {
        _.toEither
      }
      .map {
        case Right(protocol) =>
          Try {
            protocol.`type` match {

              case "player_join" =>
                protocol.payload.convertTo[PlayerJoin]

              case "move" =>
                protocol.payload
                  .convertTo[PlayerMove]

              case "chat_message" =>
                protocol.payload
                  .convertTo[ChatMessage]

              case _ =>
                Error("UnknownMessageType")
            }
          }.getOrElse(Error("INVALID PAYLOAD"))
        case Left(_) => Error("INVALID JSON")
      }
      // if error object is returned, store it in the queue.
      .divertTo(
        Sink.foreach[ClientCommand](cmd => privateRef.offer(cmd)),
        { case _: Error => true; case _ => false }
      )
      .to(publisherPort)

    // 3 - Outgoing flow
    val outgoing = subscriberPort
      .merge(privateSource)
      .buffer(64, OverflowStrategy.dropHead)
      .map(msg => TextMessage(s"JSON MESSAGE: ${msg}"))

    Flow.fromSinkAndSourceCoupled(incoming, outgoing)
  }

  private val websocketRoute =
    (pathEndOrSingleSlash & get) {
      handleWebSocketMessages(chatFlow())
    }
  Http().newServerAt("localhost", 8081).bind(websocketRoute)
}