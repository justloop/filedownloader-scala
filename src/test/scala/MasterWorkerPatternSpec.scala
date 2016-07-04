import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.scalatest._
import worker.Master.WorkCount
import worker._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
/**
  * Created by gejun on 4/7/16.
  */
object MasterWorkerPatternSpec {
  val config = ConfigFactory.load()

  class FlakyWorkExecutor extends Actor {
    var i = 0

    override def postRestart(reason: Throwable): Unit = {
      i = 3
      super.postRestart(reason)
    }

    def receive = {
      case n: Int =>
        i += 1
        if (i == 3) throw new RuntimeException("Flaky worker")
        if (i == 5) context.stop(self)

        val n2 = n * n
        val result = s"$n * $n = $n2"
        sender() ! Worker.WorkComplete(result)
    }
  }
}

class MasterWorkerPatternSpec(_system: ActorSystem)
  extends TestKit(_system)
    with Matchers
    with FlatSpecLike
    with BeforeAndAfterAll
    with ImplicitSender {

  import MasterWorkerPatternSpec._

  val workTimeout = 3.seconds
  def this() = this(ActorSystem("MasterWorkerPatternSpec", MasterWorkerPatternSpec.config))

  override def afterAll(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val allTerminated = Future.sequence(Seq(
      system.terminate()
    ))

    Await.ready(allTerminated, Duration.Inf)
  }

  "Workers" should "perform work and publish results" in {
    val master = system.actorOf(Master.props(workTimeout), name = "master")
    for (n <- 1 to 5)
      system.actorOf(Worker.props(master, Props[FlakyWorkExecutor], 1.second), "worker-" + n)

    // expect to be able to submit job within 10 sec
    within(10.seconds) {
      awaitAssert {
        master ! new WorkCount(100.toLong)
        expectMsg(Master.Ack)
      }
    }

    //send some actual work
    for (n <- 1 to 100) {
      master ! Work(n.toString, n)
      expectMsg(Master.Ack)
    }

  }
}
