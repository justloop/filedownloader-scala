package worker

import scala.collection.immutable.Queue

object WorkState {

  def empty: WorkState = WorkState(
    workNum = Long.MaxValue,//number of expected works
    allWork = Map.empty, // all the works ever received
    pendingWork = Queue.empty, // pending works need to be done by workers
    workInProgress = Map.empty, // work still performing by workers
    acceptedWorkIds = Set.empty, // the workIds that has been accepted ever since
    doneWorkIds = Set.empty, // works that has been done successfully
    failedWorkIds = Set.empty // works that has been failed
  )

  trait WorkDomainEvent
  case class WorkAccepted(work: Work) extends WorkDomainEvent
  case class WorkStarted(workId: String) extends WorkDomainEvent
  case class WorkCompleted(workId: String, result: Any) extends WorkDomainEvent
  case class WorkerFailed(workId: String) extends WorkDomainEvent
  case class WorkerTimedOut(workId: String) extends WorkDomainEvent

}

case class WorkState private (
  private var workNum: Long,
  private val allWork: Map[String,Work],
  private val pendingWork: Queue[Work],
  private val workInProgress: Map[String, Work],
  private val acceptedWorkIds: Set[String],
  private val doneWorkIds: Set[String],
  private val failedWorkIds: Set[String]) {

  import WorkState._

  def setWorkNum(num: Long) = workNum=num
  def hasWork: Boolean = pendingWork.nonEmpty
  def nextWork: Work = pendingWork.head
  def isAccepted(workId: String): Boolean = acceptedWorkIds.contains(workId)
  def isInProgress(workId: String): Boolean = workInProgress.contains(workId)
  def isDone(workId: String): Boolean = doneWorkIds.contains(workId)
  def isFailed(workId: String): Boolean = failedWorkIds.contains(workId)
  def AllDone(): Boolean = (doneWorkIds.size+failedWorkIds.size)==workNum
  def getAllFailedTasks: List[Work] = allWork.filterKeys(key => failedWorkIds.contains(key)).values.toList
  def getStatus(): String = "\nPending Work: "+pendingWork.size +"\nWork In Progress: "+workInProgress.size+"\nSucess Count: "+doneWorkIds.size+"\nFail Count: "+failedWorkIds.size

  def updated(event: WorkDomainEvent): WorkState = event match {
    case WorkAccepted(work) ⇒
      copy(
        pendingWork = pendingWork enqueue work,
        acceptedWorkIds = acceptedWorkIds + work.workId)

    case WorkStarted(workId) ⇒
      val (work, rest) = pendingWork.dequeue
      require(workId == work.workId, s"WorkStarted expected workId $workId == ${work.workId}")
      copy(
        pendingWork = rest,
        workInProgress = workInProgress + (workId -> work))

    case WorkCompleted(workId, result) ⇒
      copy(
        workInProgress = workInProgress - workId,
        doneWorkIds = doneWorkIds + workId)

    case WorkerFailed(workId) ⇒ // fail never retry
      copy(
        workInProgress = workInProgress - workId,
        failedWorkIds = failedWorkIds + workId)

    case WorkerTimedOut(workId) ⇒ // time out marked as fail too
      copy(
        workInProgress = workInProgress - workId,
        failedWorkIds = failedWorkIds + workId)
  }

}
