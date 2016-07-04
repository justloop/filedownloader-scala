package worker

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.cluster.client.ClusterClientReceptionist

import scala.concurrent.duration._

object Master {

  val ResultsTopic = "results"

  def props(workTimeout: FiniteDuration): Props =
    Props(classOf[Master], workTimeout)

  case class Ack(workId: String)

  private sealed trait WorkerStatus
  private case object Idle extends WorkerStatus
  private case class Busy(workId: String, deadline: Deadline) extends WorkerStatus
  private case class WorkerState(ref: ActorRef, status: WorkerStatus)

  private case class CleanupTick()

}

class Master(workTimeout: FiniteDuration) extends Actor with ActorLogging {
  import Master._
  import WorkState._

  ClusterClientReceptionist(context.system).registerService(self)

  // workers state is not event sourced
  private var workers = Map[String, WorkerState]()

  // workState is event sourced
  private var workState = WorkState.empty

  private val cleanupScheduleTime = 10.seconds
  private val shutdownPrepareTime = 5.seconds

  import context.dispatcher
  val cleanupTask = context.system.scheduler.schedule(cleanupScheduleTime / 2, cleanupScheduleTime / 2,
    self, new CleanupTick())

  override def postStop(): Unit = {
    cleanupTask.cancel()
    log.info("master node shutdown")
  }

  def receive = {
    case MasterWorkerProtocol.RegisterWorker(workerId) =>
      if (workers.contains(workerId)) {
        workers += (workerId -> workers(workerId).copy(ref = sender()))
      } else {
        log.info("Worker registered: {}", workerId)
        workers += (workerId -> WorkerState(sender(), status = Idle))
        if (workState.hasWork)
          sender() ! MasterWorkerProtocol.WorkIsReady
      }

    case MasterWorkerProtocol.WorkerRequestsWork(workerId) =>
      if (workState.hasWork) {
        workers.get(workerId) match {
          case Some(s @ WorkerState(_, Idle)) =>
            val work = workState.nextWork
              workState = workState.updated(WorkStarted(work.workId))
              log.info("Giving worker {} some work {}", workerId, work.workId)
              workers += (workerId -> s.copy(status = Busy(work.workId, Deadline.now + workTimeout)))
              sender() ! work
          case _ =>
        }
      }

    case MasterWorkerProtocol.WorkIsDone(workerId, workId, result) =>
      // idempotent
      if (workState.isDone(workId)) {
        // previous Ack was lost, confirm again that this is done
        sender() ! MasterWorkerProtocol.Ack(workId)
      } else if (!workState.isInProgress(workId)) {
        log.info("Work {} not in progress, reported as done by worker {}", workId, workerId)
      } else {
        log.info("Work {} is done by worker {}", workId, workerId)
        changeWorkerToIdle(workerId, workId)
        workState = workState.updated(WorkCompleted(workId, result))
        // Ack back to original sender
        sender ! MasterWorkerProtocol.Ack(workId)
        checkJobsAllDone
      }

    case MasterWorkerProtocol.WorkFailed(workerId, workId) =>
      if (workState.isInProgress(workId)) {
        log.info("Work {} failed by worker {}", workId, workerId)
        changeWorkerToIdle(workerId, workId)
        workState = workState.updated(WorkerFailed(workId))
        // Ack back to original sender
        sender ! MasterWorkerProtocol.Ack(workId)
        checkJobsAllDone
      }

    case work: Work =>
      // idempotent
      if (workState.isAccepted(work.workId)) {
        sender() ! Master.Ack(work.workId)
      } else {
        log.info("Accepted work: {}", work.workId)
        // Ack back to original sender
        sender() ! Master.Ack(work.workId)
        workState = workState.updated(WorkAccepted(work))
        notifyWorkers()
      }

    case cleanupTick:CleanupTick =>
      for ((workerId, s @ WorkerState(_, Busy(workId, timeout))) ← workers) {
        if (timeout.isOverdue) {
          log.info("Work timed out: {}", workId)
          changeWorkerToIdle(workerId, workId)
          workState = workState.updated(WorkerFailed(workId))
          // Ack back to original sender
          sender ! MasterWorkerProtocol.Ack(workId)
          checkJobsAllDone
        }
      }

    case ShutdownSystem => {
      log.info("-------------------------------Summary---------------------------------")
      log.info(workState.getStatus())
      log.info("-----------------------------------------------------------------------")
      log.info("Shutting down all workers")
      workers.foreach {
        case (id, WorkerState(ref, _)) => {
          log.info("shut down "+id)
          ref ! ShutdownSystem
        }
      }

      log.info("Shutting down all system")
      context.system.terminate()
      log.info("All shut down.")
    }
  }

  def checkJobsAllDone(): Unit = {
    //log.info(workState.getStatus())
    if (workState.AllDone()) {
      log.info("Jobs are finished, shutting down the system...")
      context.system.scheduler.scheduleOnce(shutdownPrepareTime, self, ShutdownSystem)
    }
  }

  def notifyWorkers(): Unit =
    if (workState.hasWork) {
      // could pick a few random instead of all
      workers.foreach {
        case (_, WorkerState(ref, Idle)) => ref ! MasterWorkerProtocol.WorkIsReady
        case _                           => // busy
      }
    }

  def changeWorkerToIdle(workerId: String, workId: String): Unit =
    workers.get(workerId) match {
      case Some(s @ WorkerState(_, Busy(`workId`, _))) ⇒
        workers += (workerId -> s.copy(status = Idle))
      case _ ⇒
      // ok, might happen after standby recovery, worker state is not persisted
    }
}