package com.wajam.commons

import scala.concurrent.Future

/**
 * Trait for creating identifier.
 */
trait IdGenerator[T] {
  def nextId: T
}

/**
 * Trait for getting asynchronously identifier.
 * IE: From a web service (http call)
 */
trait AsyncIdGenerator[T] {
  def nextId: Future[T]
}

/**
 * This trait should be used as a mixin to synchronize id generation for the class it is mixed in.
 */
trait SynchronizedIdGenerator[T] extends IdGenerator[T] {
  abstract override def nextId: T = synchronized {
    super.nextId
  }
}
