# Akka master worker pattern file downloader

## Feature

> Master worker pattern, workers can fail at any time, still able to proceed. Workers can work concurrently and you can specify any number of workers to absorb tasks

> Download task fault tolerance, even task download fail or time out, master able to take care

> Downloading task throttling enabled, able to control the download speed

> Support File, FTP, HTTP, HTTPS, SFTP and easy to add new protocol by extending ProtocolHandler class

> configure download urls through configurations

## Run

> You need Java 8, Scala 2.11.8 and Sbt 0.13.8

> to run, go to the home directory and sbt run

> to package into jar, go to the home directory and sbt package

> to test, go to the home directory and sbt test