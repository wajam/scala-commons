package com.wajam.commons

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.mutable.ArrayBuffer

/**
 * Util functions for Future manipulation when processing a sequence of items asynchronously.
 */
object FutureUtils {

  def serialize[A, B](elements: Seq[A], asyncProcessing: A => Future[B])
                     (implicit ec: ExecutionContext): Future[Seq[B]] = {
    serialWithTransform(elements, asyncProcessing, identity[Future[B]])
  }

  def serializeWithRecovery[A, B](elements: Seq[A], asyncProcessing: A => Future[B])
                                 (implicit ec: ExecutionContext): Future[Seq[Either[Throwable, B]]] = {
    serialWithTransform(elements, asyncProcessing, toFutureOfEither[B])
  }

  def serialWithTransform[A, B, C](elements: Seq[A], asyncProcessing: A => Future[B], transform: Future[B] => Future[C])
                                  (implicit ec: ExecutionContext): Future[Seq[C]] = {
    import scala.languageFeature.postfixOps
    elements.foldLeft(Future.successful(new ArrayBuffer(elements.size)): Future[ArrayBuffer[C]]) {
      (previousFuture, e) =>
        previousFuture.flatMap(prevResult => transform(asyncProcessing(e)).map(prevResult +=))
    }.map(_.toSeq)
  }

  def parallel[A, B](elements: Seq[A], asyncProcessing: A => Future[B])
                    (implicit ec: ExecutionContext): Future[Seq[B]] = {
    parallelWithTransform(elements, asyncProcessing, identity[Future[B]])
  }

  def parallelWithRecovery[A, B](elements: Seq[A], asyncProcessing: A => Future[B])
                                (implicit ec: ExecutionContext): Future[Seq[Either[Throwable, B]]] = {
    parallelWithTransform(elements, asyncProcessing, toFutureOfEither[B])
  }

  def parallelWithTransform[A, B, C](elements: Seq[A], asyncProcessing: A => Future[B], transform: Future[B] => Future[C])
                                 (implicit ec: ExecutionContext): Future[Seq[C]] = {
    Future.sequence {
      for {
        e <- elements
      } yield transform(asyncProcessing(e))
    }
  }

  private def toFutureOfEither[B](future: Future[B])
                                 (implicit ec: ExecutionContext): Future[Either[Throwable, B]] = {
    future.map(s => Right(s)).recover {
      case x => Left(x)
    }
  }
}
