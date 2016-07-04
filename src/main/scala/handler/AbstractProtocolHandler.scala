package handler

import java.io.{FileOutputStream, IOException, InputStream}
import java.net.MalformedURLException

import com.typesafe.scalalogging.LazyLogging
import worker.DownloadTask

/**
  * Created by gejun on 3/7/16.
  */
class AbstractProtocolHandler extends ProtocolHandler with LazyLogging{
  protected var downloadDir: String = null

  private var limit: Int = 0

  def init (downloadDir: String, limit: Int) {
    this.downloadDir = downloadDir
    this.limit = limit
  }

  /**
    * Common functionality of all Protocol Handlers, read from InputStream and write to FileOutputStream According to throttled speed
    * @param inStream
    * @param outStream
    * @param info
    * @throws MalformedURLException
    * @throws IOException
    */
  @throws[MalformedURLException]
  @throws[IOException]
  protected def process (inStream: InputStream, outStream: FileOutputStream, info: DownloadTask) {
    val inThrottleStream: ThrottledInputStream = new ThrottledInputStream (inStream, limit)
    val buf: Array[Byte] = new Array[Byte] (8192)
    var bytesread: Int = inThrottleStream.read (buf)
    var bytesBuffered: Int = 0
    var round: Int = 0
    while (bytesread > -1) {
      {
        outStream.write(buf, 0, bytesread)
        bytesBuffered += bytesread
        if (bytesBuffered > 1024 * 1024) {
          bytesBuffered = 0
          outStream.flush
          round += 1
          logger.debug("[Progress] -- " + info.url + " | " + round + "MB~")
        }
        bytesread = inThrottleStream.read(buf)
      }
    }
    outStream.flush
  }

  /**
    * Get the name of Protocol Handler
    *
    * @return Name
    */
  override def getName: String = {return "Default Protocol Handler, please extend"}

  /**
    * @param task the file to be processed
    *             Extend this method to download file
    */
  override def process(task: DownloadTask): Unit = {}
}

