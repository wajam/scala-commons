package com.wajam.asyncclient

import scala.language.higherKinds

trait ConvertableResponse[TypedResponse[_]] {
  def as[A](implicit mf: Manifest[A]): TypedResponse[A]
}

