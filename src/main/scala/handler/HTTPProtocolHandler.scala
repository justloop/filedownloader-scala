package handler

import java.io.{FileOutputStream, IOException, InputStream}
import java.net._

import org.apache.commons.io.FilenameUtils
import worker.DownloadTask

/**
  * Created by gejun on 3/7/16.
  */
class HTTPProtocolHandler extends AbstractProtocolHandler {
  override def getName: String = {
    return "HTTPProtocolHandler"
  }

  @throws[MalformedURLException]
  @throws[IOException]
  override def process(task: DownloadTask) {
    var in: InputStream = null
    var fout: FileOutputStream = null
    try {
      if (task.username.length > 0) {
        Authenticator.setDefault(new MyAuthenticator(task.username, task.password))
      }
      in = new URL(task.url).openStream
      fout = new FileOutputStream(downloadDir + FilenameUtils.getName(task.url))
      super.process(in, fout, task)
    } finally {
      if (in != null) {
        in.close
      }
      if (fout != null) {
        fout.flush
      }
    }
  }

  /**
    * In case username and password is required to download
    * @param username
    * @param password
    */
  class MyAuthenticator(var username: String, var password: String) extends Authenticator {
    override protected def getPasswordAuthentication: PasswordAuthentication = {
      val prompt: String = getRequestingPrompt
      val hostname: String = getRequestingHost
      val ipaddr: InetAddress = getRequestingSite
      val port: Int = getRequestingPort
      return new PasswordAuthentication(username, password.toCharArray)
    }
  }

}
