package com.wajam.asyncclient

import scala.language.higherKinds

/**
 * Untyped response type that can be casted to a typed case class
 * @tparam TypedResponse The response type to return
 */
trait ConvertableResponse[TypedResponse[_]] {
  /**
   * Cast to a TypedResponse handling the desired case class
   * @param mf Manifest of case class (needed by most frameworks)
   * @tparam A Desired case class
   * @return TypedResponse wrapping response info and case class
   */
  def as[A](implicit mf: Manifest[A]): TypedResponse[A]
}

