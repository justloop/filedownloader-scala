package worker

import scala.collection.immutable.Queue

object WorkState {

  def empty: WorkState = WorkState(
    allWork = Map.empty,
    pendingWork = Queue.empty,
    workInProgress = Map.empty,
    acceptedWorkIds = Set.empty,
    doneWorkIds = Set.empty,
    failedWorkIds = Set.empty)

  trait WorkDomainEvent
  case class WorkAccepted(work: Work) extends WorkDomainEvent
  case class WorkStarted(workId: String) extends WorkDomainEvent
  case class WorkCompleted(workId: String, result: Any) extends WorkDomainEvent
  case class WorkerFailed(workId: String) extends WorkDomainEvent
  case class WorkerTimedOut(workId: String) extends WorkDomainEvent

}

case class WorkState private (
  private val allWork: Map[String,Work],
  private val pendingWork: Queue[Work],
  private val workInProgress: Map[String, Work],
  private val acceptedWorkIds: Set[String],
  private val doneWorkIds: Set[String],
  private val failedWorkIds: Set[String]) {

  import WorkState._

  def hasWork: Boolean = pendingWork.nonEmpty
  def nextWork: Work = pendingWork.head
  def isAccepted(workId: String): Boolean = acceptedWorkIds.contains(workId)
  def isInProgress(workId: String): Boolean = workInProgress.contains(workId)
  def isDone(workId: String): Boolean = doneWorkIds.contains(workId)
  def isFailed(workId: String): Boolean = failedWorkIds.contains(workId)
  def AllDone(): Boolean = pendingWork.isEmpty && workInProgress.isEmpty
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
