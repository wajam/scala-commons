package com.wajam.asyncclient

import com.ning.http.client.{FluentCaseInsensitiveStringsMap, Response}

trait Headers {
  def getValue(key: String): Option[String]

  def getValues(key: String): Iterable[String]

  def toMap: Map[String, Iterable[String]]
}

object Headers {
  def apply(response: Response): Headers = ResponseHeaders(response.getHeaders)

  def apply(headers: Map[String, String]) = new Headers {
    def getValue(key: String): Option[String] = headers.get(key)

    def getValues(key: String): Iterable[String] = headers.get(key).map(List(_)).getOrElse(Nil)

    lazy val toMap: Map[String, Iterable[String]] = headers.map { case (k, v) => (k, List(v)) }
  }

  val Empty = new Headers {
    def getValue(key: String): Option[String] = None

    def getValues(key: String): Iterable[String] = Nil

    val toMap: Map[String, Iterable[String]] = Map.empty()
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