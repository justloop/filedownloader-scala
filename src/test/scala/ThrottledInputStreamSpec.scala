import java.io._

import ThrottledInputStreamSpec.CB.CB
import akka.actor.ActorLogging
import akka.testkit.TestKit
import handler.ThrottledInputStream
import org.scalatest.{BeforeAndAfterAll, FlatSpec, FlatSpecLike, Matchers}

import collection.JavaConversions._

/**
  * Created by JGE on 5/7/2016.
  */
object ThrottledInputStreamSpec {
  val BUFF_SIZE = 1024;

  object CB extends Enumeration {
    type CB = Value
    val ONE_C = Value("ONE_C")
    val BUFFER = Value("BUFFER")
    val BUFF_OFFSET = Value("BUFF_OFFSET")
  }
}

class ThrottledInputStreamSpec extends FlatSpec
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll {

  import ThrottledInputStreamSpec._

  @throws[IOException]
  def createFile(sizeInKB: Long):File = {
    val tmpFile:File = createFile
    writeToFile(tmpFile, sizeInKB)
    tmpFile
  }

  @throws[IOException]
  def createFile: File = {
    File.createTempFile("tmp", "dat")
  }

  @throws[IOException]
  def writeToFile(tmpFile: File, sizeInKB: Long) {
    val out = new FileOutputStream(tmpFile)
    try {
      val buffer = Array.ofDim[Byte](1024)
      for (index <- 0L until sizeInKB) {
        out.write(buffer)
      }
    } finally {
      if (out != null) out.close()
    }
  }

  @throws[IOException]
  def copyBytesWithOffset(in: InputStream, out: OutputStream, buffSize: Int) {
    val buf = Array.ofDim[Byte](buffSize)
    var bytesRead = in.read(buf, 0, buffSize)
    while (bytesRead >= 0) {
      out.write(buf, 0, bytesRead)
      bytesRead = in.read(buf)
    }
  }

  @throws[IOException]
  def copyByteByByte(in: InputStream, out: OutputStream) {
    var ch = in.read()
    while (ch >= 0) {
      out.write(ch)
      ch = in.read()
    }
  }

  @throws[IOException]
  def copyBytes(in: InputStream, out: OutputStream, buffSize: Int) {
      val buf = Array.ofDim[Byte](buffSize)
      var bytesRead = in.read(buf)
      while (bytesRead >= 0) {
        out.write(buf, 0, bytesRead)
        bytesRead = in.read(buf)
      }
    }

  @throws[IOException]
  def copyAndAssert(tmpFile: File,
                            outFile: File,
                            maxBandwidth: Long,
                            factor: Float,
                            sleepTime: Int,
                            flag: CB): Long = {
    import ThrottledInputStreamSpec._

    var bandwidth: Long = 0l
    var in: ThrottledInputStream = null
    val maxBPS = (maxBandwidth / factor).toLong
    in = if (maxBandwidth == 0) new ThrottledInputStream(new FileInputStream(tmpFile), Long.MaxValue) else new ThrottledInputStream(new FileInputStream(tmpFile),
      maxBPS)
    val out = new FileOutputStream(outFile)
    try {
      if (flag == CB.BUFFER) {
        copyBytes(in, out, BUFF_SIZE)
      } else if (flag == CB.BUFF_OFFSET) {
        copyBytesWithOffset(in, out, BUFF_SIZE)
      } else {
        copyByteByByte(in, out)
      }
      println(in.toString)
      bandwidth = in.getBytesPerSec
      in.getTotalBytesRead should be(tmpFile.length)
      in.getBytesPerSec > maxBandwidth / (factor * 1.2) should be (true)
      in.getTotalSleepTime > sleepTime || in.getBytesPerSec <= maxBPS should be (true)
    } finally {
      if (in != null) {
        in.close()
      }
      if (out != null) {
        out.close()
      }
    }
    bandwidth
  }

  "ThrottledInputStream" should "throttle the inputstream" in {
    var tmpFile: File = null
    var outFile: File = null
    tmpFile = createFile(1024)
    outFile = createFile
    tmpFile.deleteOnExit()
    outFile.deleteOnExit()
    val maxBandwidth = copyAndAssert(tmpFile, outFile, 0, 1, -1, CB.BUFFER)
    copyAndAssert(tmpFile, outFile, maxBandwidth, 20, 0, CB.BUFFER)
    copyAndAssert(tmpFile, outFile, maxBandwidth, 20, 0, CB.BUFF_OFFSET)
    copyAndAssert(tmpFile, outFile, maxBandwidth, 20, 0, CB.ONE_C)
  }
}
