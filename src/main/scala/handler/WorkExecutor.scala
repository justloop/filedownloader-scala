package handler

import java.net.URI

import akka.actor.{Actor, ActorLogging}
import config.JobConfig
import worker.{ DownloadTask, Worker}

class WorkExecutor extends Actor with ActorLogging{
  private var abstractProtocolHandler:AbstractProtocolHandler = null;

  def receive = {
    case task: DownloadTask => {
      log.info("Recieved download task {}", task)
      val aURL: URI = new URI(task.url)
      aURL.getScheme.toUpperCase() match {
        case "FILE" => abstractProtocolHandler = new FILEProtocolHandler
        case "FTP" => abstractProtocolHandler = new FTPProtocolHandler
        case "HTTP" => abstractProtocolHandler = new HTTPProtocolHandler
        case "HTTPS" => abstractProtocolHandler = new HTTPSProtocolHandler
        case "SFTP" => abstractProtocolHandler = new SFTPProtocolHandler
        case other => throw new UnsupportedOperationException("Protocol " + other.toString + " not supported.")
      }
      abstractProtocolHandler.init(JobConfig.downloadDir,JobConfig.limit)
      abstractProtocolHandler.process(task)
      sender() ! Worker.WorkComplete(task)
      abstractProtocolHandler = null;
    }
    case default => {
      log.warning("Unknown task received, ignored. {}", default)
      throw new UnsupportedOperationException("Task not supported")
    }
  }

}