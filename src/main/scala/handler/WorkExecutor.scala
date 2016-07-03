package handler

import akka.actor.{Actor, ActorLogging}
import worker.MasterWorkerProtocol.WorkFailed
import worker.{DownloadTask, Worker}

class WorkExecutor extends Actor with ActorLogging{

  def receive = {
    case task: DownloadTask => {
      log.info("Recieved download task {}", task)
      //TODO

      sender() ! Worker.WorkComplete(None)
    }
    case default => {
      log.warning("Unknown task received, ignored. {}", default)
      throw new UnsupportedOperationException("Task not supported")
    }
  }

}