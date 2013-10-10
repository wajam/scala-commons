package com.wajam.commons

trait Closable {
  def close()
}

object Closable {
  /**
   * Provide a convenient way to ensure `Closable` is automatically closed at the end of the `block` execution.
   */
  def using[A <: Closable, B](closable: A)(block: (A) => B): B = {
    try {
      block(closable)
    } finally {
      closable.close()
    }
  }
}
