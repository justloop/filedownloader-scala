import java.util.UUID

import akka.actor._
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import config.JobConfig
import handler.WorkExecutor
import worker._

import scala.concurrent.duration._
import collection.JavaConversions._

object Main {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()
    val system = ActorSystem("FileDownloader", conf)
    val master = startMaster(system)
    for (i <- 1 to JobConfig.numOfWorks) {
      // start workers according to configurations
      startWorker(system, master, i)
    }
    Thread.sleep(5000)
    val tasks = readJobsFromConfig
    tasks.foreach(
      task => {
        val work = Work(nextWorkId(), task)
        master ! work
      }
    )
  }

  def nextWorkId(): String = UUID.randomUUID().toString

  def workTimeout = JobConfig.workTimeout.seconds

  def startMaster(system: ActorSystem): ActorRef = {
    system.actorOf(Master.props(workTimeout), name = "master")
  }

  def startWorker(system: ActorSystem, master: ActorRef, ind: Int): Unit = {
    system.actorOf(Worker.props(master, Props[WorkExecutor]), "worker-"+ind)
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
    tasks
  }
}
