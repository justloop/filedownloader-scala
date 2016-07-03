package handler

import java.io.{FileInputStream, FileOutputStream, IOException}
import java.net.MalformedURLException

import org.apache.commons.io.FilenameUtils
import worker.DownloadTask

/**
  * Created by gejun on 3/7/16.
  */
class FILEProtocolHandler extends AbstractProtocolHandler {
  override def getName: String = {
    return "FileProtocolHandler"
  }

  @throws[MalformedURLException]
  @throws[IOException]
  override def process(task: DownloadTask) {
    var in: FileInputStream = null
    var fout: FileOutputStream = null
    try {
      in = new FileInputStream(task.url)
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
}

