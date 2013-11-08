package com.wajam.commons

import org.scalatest.FlatSpec
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.ConcurrentLinkedQueue

class TestFutureUtils extends FlatSpec with ShouldMatchers {
  val numberToFailOn = 50
  val exception = new RuntimeException("failed!")
  val elements = List(100, 20, 50, 1)

  import scala.concurrent.ExecutionContext.Implicits.global

  trait Setup {
    val recorder = new ConcurrentLinkedQueue[Int]()
  }

  "serialize" should "execute the async processing to each element one after the other" in new Setup {
    val f = FutureUtils.serialize(elements, delayedFuture(recorder))
    val result = Await.result(f, Duration.Inf)

    result should equal(elements)

    //elements should have been recorded in order
    recorder.toArray should equal(elements.toArray)
  }

  it should "fail at first failure" in new Setup {
    val f = FutureUtils.serialize(elements, delayedFuture(recorder, Some(numberToFailOn)))

    evaluating {
      Await.result(f, Duration.Inf)
    } should produce[RuntimeException]
  }

  "serializeWithRecovery" should "process all elements even if one fail" in new Setup {
    val f = FutureUtils.serializeWithRecovery(elements, delayedFuture(recorder, Some(numberToFailOn)))

    val result = Await.result(f, Duration.Inf)
    result should equal(List(Right(100), Right(20), Left(exception), Right(1)))
  }

  "parallel" should "execute the async processing to each element one after the other" in new Setup {
    val f = FutureUtils.parallel(elements, delayedFuture(recorder))
    val result = Await.result(f, Duration.Inf)

    // Since each item is delayed by its value, if all tasks are executed in parallel,
    // then elements should have been recorded from the smallest to the largest.
    recorder.toArray should equal(elements.sorted.toArray)
  }

  it should "fail at first failure" in new Setup {
    val f = FutureUtils.parallel(elements, delayedFuture(recorder, Some(numberToFailOn)))

    evaluating {
      Await.result(f, Duration.Inf)
    } should produce[RuntimeException]
  }

  "paralellWithRecovery" should "process all elements even if one fail" in new Setup {
    val f = FutureUtils.parallelWithRecovery(elements, delayedFuture(recorder, Some(numberToFailOn)))

    val result = Await.result(f, Duration.Inf)
    result should equal(List(Right(100), Right(20), Left(exception), Right(1)))
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
