package com.wajam.commons.hbase

import java.security.MessageDigest
import scala.language.implicitConversions

trait Key {
  def string: String

  def min: Key = StringKey.Min

  def max: Key = StringKey.Max

  def head: Key
}

case class CompoundKey(a: Key, b: Key) extends Key {
  override def string: String = s"${a.string}_${b.string}"

  override def min: Key = CompoundKey(a, b.min)

  override def max: Key = CompoundKey(a, b.max)

  def head: Key = a.head
}

object CompoundKey {
  def apply(first: Key, second: Key, rest: Key*): CompoundKey = rest.foldLeft(CompoundKey(first, second)) {
    case (out, k) => CompoundKey(out, k)
  }
}

case class StringKey(v: String) extends Key {
  override def string: String = v

  def head: Key = this
}

object StringKey {
  val Min = StringKey("!")

  val Max = StringKey("~")
}

case class LongKey(v: Long, pad: Int = 20) extends Key {
  override def string: String = s"%0${pad}d".format(v)

  def head: Key = this
}

case class HashKey(v: String) extends Key {
  override val string: String = MessageDigest.getInstance("MD5").digest(v.getBytes).map("%02x".format(_)).mkString

  override def min: Key = HashKey.Min

  override def max: Key = HashKey.Max

  def head: Key = this
}

object HashKey {
  def apply(v: Long): HashKey = new HashKey(v.toString)

  val Min = StringKey("0")

  val Max = StringKey("g")
}

trait KeyImplicits {

  implicit def string2key(v: String) = StringKey(v)

  implicit def long2key(v: Long) = LongKey(v)

}

object KeyImplicits extends KeyImplicits
