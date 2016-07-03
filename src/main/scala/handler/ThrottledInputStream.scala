package handler

import java.io.{IOException, InputStream}
import javax.annotation.PostConstruct

/**
  * Created by gejun on 3/7/16.
  */
class ThrottledInputStream(val rawInStream: InputStream,val maxInBytesPerSec: Long) extends InputStream {
  private val rawStream: InputStream = rawInStream
  private val maxBytesPerSec: Long = maxInBytesPerSec
  private val startTime: Long = System.currentTimeMillis
  private var bytesRead: Long = 0
  private var totalSleepTime: Long = 0
  private val SLEEP_DURATION_MS: Long = 50

  @throws[IOException]
  override def read: Int = {
    throttle
    val data: Int = rawStream.read
    if (data != -1) {
      bytesRead += 1
    }
    return data
  }

  @throws[IOException]
  override def read(b: Array[Byte]): Int = {
    throttle
    val readLen: Int = rawStream.read(b)
    if (readLen != -1) {
      bytesRead += readLen
    }
    return readLen
  }

  @throws[IOException]
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    throttle
    val readLen: Int = rawStream.read(b, off, len)
    if (readLen != -1) {
      bytesRead += readLen
    }
    return readLen
  }

  @throws[IOException]
  def throttle {
    if (getBytesPerSec > maxBytesPerSec) {
      try {
        Thread.sleep(SLEEP_DURATION_MS)
        totalSleepTime += SLEEP_DURATION_MS
      }
      catch {
        case e: InterruptedException => {
          throw new IOException("Thread aborted", e)
        }
      }
    }
  }

  def getTotalBytesRead: Long = {
    return bytesRead
  }

  def getBytesPerSec: Long = {
    val elapsed: Long = (System.currentTimeMillis - startTime) / 1000
    if (elapsed == 0) {
      return bytesRead
    }
    else {
      return bytesRead / elapsed
    }
  }

  def getTotalSleepTime: Long = {
    return totalSleepTime
  }

  override def toString: String = {
    return "ThrottledInputStream{" + "bytesRead=" + bytesRead + ", maxBytesPerSec=" + maxBytesPerSec + ", bytesPerSec=" + getBytesPerSec + ", totalSleepTime=" + totalSleepTime + '}'
  }
}
