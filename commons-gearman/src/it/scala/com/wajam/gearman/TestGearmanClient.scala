package com.wajam.gearman

import scala.concurrent.{Await, ExecutionContext}
import java.util.concurrent.{TimeoutException, Executors}
import com.wajam.gearman.impl.AsyncGearmanClient
import scala.util.{Failure, Success}
import com.wajam.gearman.exception.{JobSubmissionException, JobExecutionException}
import scala.concurrent.duration._

class TestGearmanClient extends GearmanIntegrationTest {

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))

  val client: GearmanClient = new AsyncGearmanClient(Seq("127.0.0.1"))

  val notWorkingClient: GearmanClient = new AsyncGearmanClient(Seq("nowhere.does.not.exist.nothing.none"))

  "Working client" should "not fail when executeJob success data" in {
    val data = Map("success" -> "success")
    val f = client.executeJob(testFunctionName, data)
    f.onComplete {
      case Success(v) => data should equal(v)
      case Failure(e) => fail("Success data should return a success")
    }
    Await.ready(f, 2.seconds)
  }

  it should "fail when executeJob fail data" in {
    val data = Map("fail" -> "fail")
    val f = client.executeJob(testFunctionName, data)
    evaluating(Await.result(f, 2.seconds)) should produce[JobExecutionException]
  }

  it should "not fail when enqueueJob success data" in {
    val data = Map("success" -> "success")
    val f = client.enqueueJob(testFunctionName, data)
    f.onComplete {
      case Success(v) => data should equal(v)
      case Failure(e) => fail("Success data should return a success")
    }
    Await.ready(f, 2.seconds)
  }

  it should "not fail when enqueueJob fail data" in {
    val data = Map("fail" -> "fail")
    val f = client.enqueueJob(testFunctionName, data)
    f.onComplete {
      case Success(v) => data should equal(v)
      case Failure(e) => fail("Success data should return a success")
    }
    Await.ready(f, 2.seconds)
  }

  "Non working client" should "throw exception on first queuing/execute and then queue all others without executing them" in {
    val data = Map("success" -> "success")

    val f1 = notWorkingClient.executeJob(testFunctionName, data)
    evaluating(Await.result(f1, 2.seconds)) should produce[JobSubmissionException]

    val f2 = notWorkingClient.executeJob(testFunctionName, data)
    evaluating(Await.ready(f2, 2.seconds)) should produce[TimeoutException]

    val f3 = notWorkingClient.enqueueJob(testFunctionName, data)
    evaluating(Await.ready(f3, 2.seconds)) should produce[TimeoutException]
  }

  override def afterAll() = {
    client.shutdown()
    super.afterAll()
  }

}
