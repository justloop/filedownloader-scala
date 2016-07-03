package handler

import java.io.IOException
import java.net.URISyntaxException

import worker.DownloadTask

/**
  * Created by gejun on 3/7/16.
  */
trait ProtocolHandler {
  /**
    * Get the name of Protocol Handler
    *
    * @return Name
    */
  def getName: String

  /**
    * @param task the file to be processed
    *             Extend this method to download file
    */
  @throws[IOException]
  @throws[URISyntaxException]
  def process (task: DownloadTask)
}