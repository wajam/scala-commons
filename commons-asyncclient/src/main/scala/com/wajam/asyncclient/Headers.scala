package com.wajam.asyncclient

import com.ning.http.client.FluentCaseInsensitiveStringsMap

trait Headers {
  def getValue(key: String): Option[String]

  def getValues(key: String): Iterable[String]

  def toMap: Map[String, Iterable[String]]
}

object Headers {
  def apply(headers: Map[String, String]): Headers = SimpleValueMapHeaders(headers)

  def apply(headers: FluentCaseInsensitiveStringsMap): Headers = ResponseHeaders(headers)

  val Empty = new Headers {
    def getValue(key: String): Option[String] = None

    def getValues(key: String): Iterable[String] = Nil

    def toMap: Map[String, Iterable[String]] = Map.empty()
  }

  private case class SimpleValueMapHeaders(headers: Map[String, String]) extends Headers {
    def getValue(key: String): Option[String] = headers.get(key)

    def getValues(key: String): Iterable[String] = headers.get(key).map(List(_)).getOrElse(Nil)

    lazy val toMap: Map[String, Iterable[String]] = headers.map { case (k, v) => (k, List(v)) }
  }

  private case class ResponseHeaders(headers: FluentCaseInsensitiveStringsMap) extends Headers {
    import scala.collection.JavaConversions._

    def getValue(key: String): Option[String] = Option(headers.getFirstValue(key))

    def getValues(key: String): Iterable[String] = {
      Option[Iterable[String]](headers.get(key)).getOrElse(Nil)
    }

    lazy val toMap: Map[String, Iterable[String]] = {
      headers.entrySet().map(entry => entry.getKey -> entry.getValue.toIterable).toMap
    }
  }
}