package com.wajam.commons.elasticsearch

import java.nio.charset.Charset

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization

trait ElasticsearchSerializer {

  def charset: Charset

  def serialize[A <: AnyRef](a: A): Array[Byte]

  def deserialize[A](obj: Array[Byte])(implicit mf: Manifest[A]): A

}

class ElasticsearchJsonSerializer extends ElasticsearchSerializer {

  implicit val formats = DefaultFormats

  val charset = Charset.forName("UTF-8")

  def serialize[A <: AnyRef](a: A): Array[Byte] = {
    val json = Serialization.write(a)
    json.getBytes(charset)
  }

  def deserialize[A](obj: Array[Byte])(implicit mf: Manifest[A]): A = {
    val json = new String(obj, charset)
    parse(json).extract[A]
  }

}
