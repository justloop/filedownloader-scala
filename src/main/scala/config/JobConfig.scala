package config

import com.typesafe.config.ConfigFactory

/**
  * Created by gejun on 3/7/16.
  */
object JobConfig {
  val config =  ConfigFactory.load("jobs.conf")
  lazy val root = config.getConfig("jobs")
  lazy val tasks = root.getStringList("urls")
  lazy val workTimeout = root.getInt("timeout")
  lazy val numOfWorks = root.getInt("workers")
  lazy val downloadDir = root.getString("download_dir")
  lazy val limit = root.getInt("limit")
}
