package handler

import java.io.{FileOutputStream, IOException, InputStream}
import java.net.{URI, URISyntaxException}

import com.jcraft.jsch.{Channel, ChannelSftp, JSch, Session}
import org.apache.commons.io.FilenameUtils
import org.slf4j.{Logger, LoggerFactory}
import worker.DownloadTask
import java.util.Properties

/**
  * Created by gejun on 3/7/16.
  */
class SFTPProtocolHandler extends AbstractProtocolHandler {
  override def getName: String = {
    return "FTPProtocolHandler"
  }

  @throws[IOException]
  @throws[URISyntaxException]
  override def process(task: DownloadTask) {
    val aUrl: URI = new URI(task.url)
    var in: InputStream = null
    var fout: FileOutputStream = null
    var session: Session = null
    var channel: Channel = null
    var channelSftp: ChannelSftp = null
    try {
      val jsch: JSch = new JSch
      session = jsch.getSession(task.username, aUrl.getHost, aUrl.getPort)
      session.setPassword(task.password)
      val config: Properties = new Properties
      config.put("StrictHostKeyChecking", "no")
      session.setConfig(config)
      session.connect
      channel = session.openChannel("sftp")
      channel.connect
      channelSftp = channel.asInstanceOf[ChannelSftp]
      in = channelSftp.get(aUrl.getPath)
      fout = new FileOutputStream(downloadDir + FilenameUtils.getName(task.url))
      super.process(in, fout, task)
    }
    catch {
      case e: Any => {
        logger.error("Error with Jsch client " + e.getMessage, e)
        throw new IOException("Error with Jsch client")
      }
    } finally {
      if (in != null) {
        in.close
      }
      if (fout != null) {
        fout.flush
      }
      if (channelSftp != null) {
        channelSftp.disconnect
      }
      if (channel != null) {
        channel.disconnect
      }
      if (session != null) {
        session.disconnect
      }
    }
  }
}
