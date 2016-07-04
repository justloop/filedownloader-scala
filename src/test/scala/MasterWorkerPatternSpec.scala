import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.client.ClusterClientReceptionist
import akka.cluster.pubsub.DistributedPubSubMediator.{CurrentTopics, GetTopics, Subscribe, SubscribeAck}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
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
  val ResultsTopic = "results"

  class FlakyWorkExecutor(system: ActorSystem) extends Actor with ActorLogging{
    val mediator = DistributedPubSub(system).mediator
    ClusterClientReceptionist(system).registerService(self)

    var i = 0

    override def postRestart(reason: Throwable): Unit = {
      i = 1
      super.postRestart(reason)
    }

    def receive = {
      case n: Int =>
        i += 1
        if (i == 1) throw new RuntimeException("Flaky worker")
        if (i == 2) context.stop(self)

        val n2 = n + n
        val result = s"$n * $n = $n2"
        sender() ! Worker.WorkComplete(result)
        mediator ! DistributedPubSubMediator.Publish(ResultsTopic, WorkResult(s"$i", result))
        log.info("Result sent " + i)
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
      system.actorOf(Worker.props(master, Props(new FlakyWorkExecutor(system)), 1.second), "worker-" + n)

    val results = TestProbe()
    DistributedPubSub(system).mediator ! Subscribe(ResultsTopic, results.ref)
    expectMsgType[SubscribeAck]

    // make sure pub sub topics are replicated over to the backend system before triggering any work
    within(10.seconds) {
      awaitAssert {
        DistributedPubSub(system).mediator ! GetTopics
        expectMsgType[CurrentTopics].getTopics() should contain(ResultsTopic)
      }
    }

    // expect to be able to submit job within 10 sec
    within(10.seconds) {
      awaitAssert {
        val workCount = new WorkCount(100.toLong)
        master ! workCount
        expectMsg(Master.Ack(workCount.toString))
      }
    }

    //send some actual work
    for (n <- 1 to 100) {
      master ! Work(n.toString, n)
      expectMsg(Master.Ack(n.toString))
    }

    results.within(100.seconds) {
      val ids = results.receiveN(99).map { case WorkResult(workId, _) => workId }
      // nothing lost, and no duplicates
      ids.toVector.map(_.toInt).sorted should be((3 to 100).toVector)
    }
  }
}
