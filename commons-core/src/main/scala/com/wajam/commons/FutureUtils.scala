package com.wajam.commons

import scala.concurrent.{ ExecutionContext, Future }
import scala.collection.mutable.ArrayBuffer
import scala.util.{ Failure, Success, Try }

/**
 * Util functions for Future manipulation when processing a sequence of items asynchronously.
 */
object FutureUtils {

  /**
   * Apply an asynchronous processing to each element of an Iterable. Elements are process in sequence but in a
   * non-blocking way.
   *
   * The first failure will fail the returned Future.
   */
  def serialize[A, B](elements: Seq[A], asyncProcessing: A => Future[B])(implicit ec: ExecutionContext): Future[Seq[B]] = {
    serialWithTransform(elements, asyncProcessing, identity[Future[B]])
  }

  /**
   * Apply an asynchronous processing to each element of an Iterable. Elements are process in sequence but in a
   * non-blocking way.
   *
   * Any success or failure is wrapped in a Try object. The returned Future will contain one Try object per
   * element in the Iterable.
   */
  def serializeWithRecovery[A, B](elements: Seq[A], asyncProcessing: A => Future[B])(implicit ec: ExecutionContext): Future[Seq[Try[B]]] = {
    serialWithTransform(elements, asyncProcessing, toFutureOfTry[B])
  }

  def serialWithTransform[A, B, C](elements: Seq[A], asyncProcessing: A => Future[B], transform: Future[B] => Future[C])(implicit ec: ExecutionContext): Future[Seq[C]] = {
    import scala.languageFeature.postfixOps
    elements.foldLeft(Future.successful(new ArrayBuffer[C](elements.size))) {
      (previousFuture, e) =>
        previousFuture.flatMap(prevResult => transform(asyncProcessing(e)).map(prevResult +=))
    }.map(_.toSeq)
  }

  /**
   * Apply an asynchronous processing to each element of an Iterable. Elements are process in parallel in a
   * non-blocking way.
   *
   * The first failure will fail the returned Future.
   */
  def parallel[A, B](elements: Seq[A], asyncProcessing: A => Future[B])(implicit ec: ExecutionContext): Future[Seq[B]] = {
    parallelWithTransform(elements, asyncProcessing, identity[Future[B]])
  }

  /**
   * Apply an asynchronous processing to each element of an Iterable. Elements are process in parallel in a
   * non-blocking way.
   *
   * Any success or failure is wrapped in a Try object. The returned Future will contain one Try object per
   * element in the Iterable.
   */
  def parallelWithRecovery[A, B](elements: Seq[A], asyncProcessing: A => Future[B])(implicit ec: ExecutionContext): Future[Seq[Try[B]]] = {
    parallelWithTransform(elements, asyncProcessing, toFutureOfTry[B])
  }

  def parallelWithTransform[A, B, C](elements: Seq[A], asyncProcessing: A => Future[B], transform: Future[B] => Future[C])(implicit ec: ExecutionContext): Future[Seq[C]] = {
    Future.sequence {
      for {
        e <- elements
      } yield transform(asyncProcessing(e))
    }
  }

  private def toFutureOfTry[B](future: Future[B])(implicit ec: ExecutionContext): Future[Try[B]] = {
    future.map(s => Success(s)).recover {
      case x => Failure(x)
    }
  }
}
