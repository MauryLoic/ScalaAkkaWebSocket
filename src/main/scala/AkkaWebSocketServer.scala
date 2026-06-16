import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, MergeHub}

object AkkaWebSocketServer extends App {
  implicit val system: ActorSystem = ActorSystem()

private case class ChatMessage(id: String, message: String)

  private val mergeHub = MergeHub.source[ChatMessage]
  private val broadcast = BroadcastHub.sink[ChatMessage]
  private val (publisherPort, subscriberPort) = mergeHub.toMat(broadcast)(Keep.both).run()

  private def chatFlow(sender: String): Flow[Message, Message, NotUsed] = {
    val incoming = Flow[Message]
      .collect { case tm: TextMessage.Strict => tm.text }
      .map(text => ChatMessage(sender, text))
      .to(publisherPort)

    val outgoing = subscriberPort
      .buffer(64, OverflowStrategy.dropHead)
      .map(msg => TextMessage(s"${msg.id}: ${msg.message}"))

    Flow.fromSinkAndSourceCoupled(incoming, outgoing)
  }

  private val websocketRoute =
    (pathEndOrSingleSlash & get) {
      parameter("pseudo") { pseudo =>
        handleWebSocketMessages(chatFlow(pseudo))
      }
    }
  Http().newServerAt("localhost", 8081).bind(websocketRoute)
}
