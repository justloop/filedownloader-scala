package producer

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill}
import com.typesafe.config.ConfigFactory
import config.JobConfig
import worker.{DownloadTask, Frontend, Work}

import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import collection.JavaConversions._

object WorkProducer {
  case class GetTasks()
}

class WorkProducer(frontend: ActorRef) extends Actor with ActorLogging {
  import WorkProducer._
  import context.dispatcher
  def scheduler = context.system.scheduler
  def rnd = ThreadLocalRandom.current
  def nextWorkId(): String = UUID.randomUUID().toString

  override def preStart(): Unit =
    scheduler.scheduleOnce(5.seconds, self, new GetTasks)

  // override postRestart so we don't call preStart and schedule a new Tick
  override def postRestart(reason: Throwable): Unit = ()

  override def postStop(): Unit = log.info("Producer was shutdown.")

  var numberOfTasks = 0;
  var acceptedTasks = 0;

  def receive = {
    case getTasks:GetTasks =>
      log.info("Get tasks from config ...")
      val tasks = readJobsFromConfig
      numberOfTasks = tasks.length
      tasks.foreach(
        task => {
          val work = Work(nextWorkId(), task)
          frontend ! work
        }
      )
    case okWork: Frontend.Ok =>
      log.info("Work is accepted: {}", okWork)
      acceptedTasks+=1
      if(numberOfTasks == acceptedTasks) {
        log.info("All tasks has been accepted by master, shutting down work producer...")
        self ! PoisonPill
      }
    case notOkWork: Frontend.NotOk =>
      log.info("Work not accepted, retry after a while: {}", notOkWork)
      scheduler.scheduleOnce(3.seconds, frontend, notOkWork)

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