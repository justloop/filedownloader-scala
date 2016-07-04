package worker

case class Work(workId: String, job: Any)

case class DownloadTask(url: String, username: String, password: String)

case class WorkResult(workId: String, result: Any)