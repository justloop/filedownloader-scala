package producer

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill}
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import config.JobConfig
import worker.Master.Reinitialize
import worker.{DownloadTask, Master, Work}

import akka.pattern._
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import collection.JavaConversions._

object WorkProducer {
  case class Ok(work: Work)
  case class NotOk(work: Work)
  case class ResetStatus()
  case class GetTasks()
}

class WorkProducer extends Actor with ActorLogging {
  import WorkProducer._
  import context.dispatcher

  val masterProxy = context.actorOf(
    ClusterSingletonProxy.props(
      settings = ClusterSingletonProxySettings(context.system).withRole("backend"),
      singletonManagerPath = "/user/master"
    ),
    name = "masterProxy")
  def scheduler = context.system.scheduler
  def rnd = ThreadLocalRandom.current
  def nextWorkId(): String = UUID.randomUUID().toString

  override def preStart(): Unit =
    scheduler.scheduleOnce(5.seconds, self, new ResetStatus)

  // override postRestart so we don't call preStart and schedule a new Tick
  override def postRestart(reason: Throwable): Unit = ()

  override def postStop(): Unit = log.info("Producer was shutdown.")

  var numberOfTasks = 0;
  var acceptedTasks = 0;

  def receive = {
    case resetStatus:ResetStatus =>
      log.info("Reinitialize State ...")
      masterProxy ! new Reinitialize()
      scheduler.scheduleOnce(5.seconds, self, new GetTasks)

    case getTasks:GetTasks =>
      log.info("Get tasks from config ...")
      val tasks = readJobsFromConfig
      numberOfTasks = tasks.length
      tasks.foreach(
        task => {
          val work = Work(nextWorkId(), task)
          implicit val timeout = Timeout(5.seconds)
          (masterProxy ? work) map {
            case Master.Ack(_) => new Ok(work)
          } recover { case _ => new NotOk(work) } pipeTo sender()
        }
      )
    case okWork: Ok =>
      log.info("Work is accepted: {}", okWork)
      acceptedTasks+=1
      if(numberOfTasks == acceptedTasks) {
        log.info("All tasks has been accepted by master, shutting down work producer...")
        self ! PoisonPill
      }
    case notOkWork: NotOk =>
      log.info("Work not accepted, retry after a while: {}", notOkWork)
      scheduler.scheduleOnce(3.seconds, self, notOkWork)

  }

  def readJobsFromConfig: List[DownloadTask] = {
    val tasks = JobConfig.tasks.map(
      line => {
        val lineArr = line.split(";")
        if (lineArr.length == 3) {
          new DownloadTask(lineArr(0),lineArr(1),lineArr(2))
        } else if(lineArr.length == 1) {
          new DownloadTask(lineArr(0),"","")
        }
      }
    ).toList.asInstanceOf[List[DownloadTask]]
    log.info("Get {} tasks from config", tasks.length )
    tasks
  }

}