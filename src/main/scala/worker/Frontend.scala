package worker

import akka.actor.Actor
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.duration._

object Frontend {
  case class Ok(work: Work)
  case class NotOk(work: Work)
}

class Frontend extends Actor {
  import Frontend._
  import context.dispatcher
  val masterProxy = context.actorOf(
    ClusterSingletonProxy.props(
      settings = ClusterSingletonProxySettings(context.system).withRole("backend"),
      singletonManagerPath = "/user/master"
    ),
    name = "masterProxy")

  def receive = {
    case work:Work =>
      implicit val timeout = Timeout(5.seconds)
      (masterProxy ? work) map {
        case Master.Ack(_) => new Ok(work)
      } recover { case _ => new NotOk(work) } pipeTo sender()

  }

}