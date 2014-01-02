package com.wajam.commons

import org.scalatest.FlatSpec
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.concurrent.duration.Duration
import org.scalatest.Matchers
import java.util.concurrent.{Executors, ConcurrentLinkedQueue}
import scala.util.{Failure, Success, Try}

class TestFutureUtils extends FlatSpec with Matchers {
  val numberToFailOn = 500
  val exception = new RuntimeException("failed!")
  val elements = List(100, 200, 500, 1)

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))

  trait Setup {
    val recorder = new ConcurrentLinkedQueue[Int]()
  }

  "serialize" should "execute the async processing to each element one after the other" in new Setup {
    val f = FutureUtils.serialize(elements, delayedFuture(recorder))
    val result = Await.result(f, Duration.Inf)

    result should equal(elements)

    //Elements should have been recorded in order.
    recorder.toArray should equal(elements.toArray)
  }

  it should behave like common(FutureUtils.serialize[Int, Int])
  it should behave like commonWithRecovery(FutureUtils.serializeWithRecovery[Int, Int])

  "parallel" should "execute the async processing to each element in parallel" in new Setup {
    val f = FutureUtils.parallel(elements, delayedFuture(recorder))
    val result = Await.result(f, Duration.Inf)

    // Since each item is delayed by its value, if all tasks are executed in parallel,
    // then elements should have been recorded from the smallest to the largest.
    recorder.toArray should equal(elements.sorted.toArray)
  }

  it should behave like common(FutureUtils.parallel[Int, Int])
  it should behave like commonWithRecovery(FutureUtils.parallelWithRecovery[Int, Int])

  private def common(f: (Seq[Int], Int => Future[Int]) => Future[Iterable[Int]]) {

    it should "fail at first failure" in new Setup {
      val future = f(elements, delayedFuture(recorder, Some(numberToFailOn)))

      evaluating {
        Await.result(future, Duration.Inf)
      } should produce[RuntimeException]
    }

    it should "support empty lists" in new Setup {
      val future = f(Nil, delayedFuture(recorder))
      val result = Await.result(future, Duration.Inf)
      result should equal(Nil)
    }
  }

  private def commonWithRecovery(f: (Seq[Int], Int => Future[Int]) => Future[Iterable[Try[Int]]]) {

    it should "process all elements even if one fail" in new Setup {
      val future = f(elements, delayedFuture(recorder, Some(numberToFailOn)))

      val result = Await.result(future, Duration.Inf)
      result should equal(List(Success(100), Success(200), Failure(exception), Success(1)))
    }
  }

  private def delayedFuture(recorder: ConcurrentLinkedQueue[Int], failOn: Option[Int] = None)
                           (e: Int): Future[Int] = {
    Future {
      Thread.sleep(e)
      failOn.map {
        case fail if e == fail => throw exception
        case _ =>
      }
      recorder.add(e)
      e
    }
  }

}
