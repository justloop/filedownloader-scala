package handler

import java.io.{FileOutputStream, IOException, InputStream}
import java.net.{URI, URISyntaxException}

import org.apache.commons.io.FilenameUtils
import org.apache.commons.net.ftp.{FTP, FTPClient, FTPReply}
import worker.DownloadTask

/**
  * Created by gejun on 3/7/16.
  */
class FTPProtocolHandler extends AbstractProtocolHandler {

  override def getName: String = {
    return "FTPProtocolHandler"
  }

  @throws[IOException]
  @throws[URISyntaxException]
  override def process(task: DownloadTask) {
    val aUrl: URI = new URI(task.url)
    var in: InputStream = null
    var fout: FileOutputStream = null
    var client: FTPClient = null
    try {
      client = getFTPClient(aUrl.getHost, task.username, task.password, if (aUrl.getPort > 0) aUrl.getPort
      else 21)
      in = client.retrieveFileStream(aUrl.getPath)
      fout = new FileOutputStream(downloadDir + FilenameUtils.getName(task.url))
      super.process(in, fout, task)
    } finally {
      if (in != null) {
        in.close
      }
      if (fout != null) {
        fout.flush
      }
      if (client != null) {
        client.logout
        client.disconnect
      }
    }
  }

  @throws[IOException]
  def getFTPClient(ftpHost: String, ftpPassword: String, ftpUserName: String, ftpPort: Int): FTPClient = {
    var tmpFtpUserName = ftpUserName
    if (ftpUserName.length < 1) tmpFtpUserName = "anonymous"
    val ftpClient: FTPClient = new FTPClient
    ftpClient.connect(ftpHost, ftpPort)
    if (!ftpClient.login(tmpFtpUserName, ftpPassword)) {
      throw new IOException("not able to conect to server " + ftpHost)
    }
    if (!FTPReply.isPositiveCompletion(ftpClient.getReplyCode)) {
      logger.error("Username or Password wrong, disconnect...")
      ftpClient.disconnect
      throw new IOException("username password not correct!")
    }
    ftpClient.enterLocalPassiveMode
    ftpClient.setBufferSize(8192)
    ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
    ftpClient.setAutodetectUTF8(true)
    return ftpClient
  }
}
