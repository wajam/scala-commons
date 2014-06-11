package com.wajam.commons

import language.reflectiveCalls
import language.implicitConversions

trait Closable {
  def close()
}

object Closable {

  implicit def withClose2Closable[A <: { def close(): Unit }](withClose: A): Closable = {
    new Closable {
      def close() = withClose.close()
    }
  }

  /**
   * Provide a convenient way to ensure `Closable` is automatically closed at the end of the `block` execution.
   */
  def using[A <% Closable, B](closable: A)(block: (A) => B): B = {
    try {
      block(closable)
    } finally {
      closable.close()
    }
  }
}
