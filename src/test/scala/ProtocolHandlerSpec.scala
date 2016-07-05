import java.io.File

import config.JobConfig
import org.apache.commons.io.FilenameUtils
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.time.Span

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Created by JGE on 5/7/2016.
  */
object ProtocolHandlerSpec {
  val downloadDir = JobConfig.downloadDir
}

class ProtocolHandlerSpec extends FlatSpec
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll
  with Eventually
  with IntegrationPatience {

  import ProtocolHandlerSpec._

  def deleteFile(file: String) {
    val location = downloadDir+ FilenameUtils.getName(file)
    val deleteFile = new File(location)
    if (deleteFile.exists()) {
      deleteFile.delete()
    }
  }

  override def afterAll(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    println("Clear all files...")
    deleteFile(downloadDir+"index.html")
    deleteFile(downloadDir+"readme.txt")
    deleteFile(downloadDir+"jupload.zip")
    deleteFile(downloadDir+"a.zip")
  }

  Main.main(Array[String]())

  def AssertFileExists = {
    val file1: File= new File(downloadDir+"index.html")
    file1.exists() should be (true)

    val file2: File = new File(downloadDir+"readme.txt")
    file2.exists() should be (true)

    val file3: File = new File(downloadDir+"jupload.zip")
    file3.exists() should be (true)

    val file4: File = new File(downloadDir+"a.zip")
    file4.exists() should be (false)
  }

  "Downloader" should "download files and store in the location" in {
    eventually(timeout(2 minutes), interval(100 milliseconds)) {
      AssertFileExists
    }
  }
}
