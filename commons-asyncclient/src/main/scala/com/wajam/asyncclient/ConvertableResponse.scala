package com.wajam.asyncclient

import net.liftweb.json.Formats
import scala.language.higherKinds

trait ConvertableResponse[TypedResponse[_]] {
  def as[A](implicit mf: Manifest[A], formats: Formats): TypedResponse[A]
}

