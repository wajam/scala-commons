package com.wajam.gearman

import scala.concurrent.{Await, ExecutionContext}
import java.util.concurrent.{TimeoutException, Executors}
import com.wajam.gearman.impl.AsyncWajamGearmanClient
import scala.util.{Failure, Success}
import com.wajam.gearman.exception.{JobSubmissionException, JobExecutionException}
import scala.concurrent.duration._

class TestWajamGearmanClient extends GearmanIntegrationTest {

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))

  val client: WajamGearmanClient = new AsyncWajamGearmanClient(Seq("127.0.0.1"))

  val notWorkingClient: WajamGearmanClient = new AsyncWajamGearmanClient(Seq("nowhere.does.not.exist.nothing.none"))

  "Working client" should "not fail when executeJob success data" in {
    val data = Map("success" -> "success")
    val f = client.executeJob(TEST_FUNCTION_NAME, data)
    f.onComplete {
      case Success(v) => data should equal(v)
      case Failure(e) => fail("Success data should return a success")
    }
    Await.ready(f, 2.seconds)
  }

  "Working client" should "fail when executeJob fail data" in {
    val data = Map("fail" -> "fail")
    val f = client.executeJob(TEST_FUNCTION_NAME, data)
    evaluating(Await.result(f, 2.seconds)) should produce[JobExecutionException]
  }

  "Working client" should "not fail when enqueueJob success data" in {
    val data = Map("success" -> "success")
    val f = client.enqueueJob(TEST_FUNCTION_NAME, data)
    f.onComplete {
      case Success(v) => data should equal(v)
      case Failure(e) => fail("Success data should return a success")
    }
    Await.ready(f, 2.seconds)
  }

  "Working client" should "not fail when enqueueJob fail data" in {
    val data = Map("fail" -> "fail")
    val f = client.enqueueJob(TEST_FUNCTION_NAME, data)
    f.onComplete {
      case Success(v) => data should equal(v)
      case Failure(e) => fail("Success data should return a success")
    }
    Await.ready(f, 2.seconds)
  }

  "Non working client" should "throw exception on first queuing/execute and then queue all others without executing them" in {
    val data = Map("success" -> "success")

    val f1 = notWorkingClient.executeJob(TEST_FUNCTION_NAME, data)
    evaluating(Await.result(f1, 2.seconds)) should produce[JobSubmissionException]

    val f2 = notWorkingClient.executeJob(TEST_FUNCTION_NAME, data)
    evaluating(Await.ready(f2, 2.seconds)) should produce[TimeoutException]

    val f3 = notWorkingClient.enqueueJob(TEST_FUNCTION_NAME, data)
    evaluating(Await.ready(f3, 2.seconds)) should produce[TimeoutException]
  }

  override def afterAll() = {
    client.shutdown()
    super.afterAll()
  }

}
