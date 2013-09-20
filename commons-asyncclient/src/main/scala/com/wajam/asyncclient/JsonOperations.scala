package com.wajam.asyncclient

import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.Serialization.write
import com.ning.http.client.Response
import dispatch.as
import java.io.{OutputStreamWriter, ByteArrayOutputStream}
import net.liftweb.json.{JsonParser, Formats}
import scala.util.Try

trait JsonOperations {

  protected def charset: String
  implicit protected def formats: Formats

  implicit object JsonRequestable extends RequestHandler[JValue] {
    val contentType = "application/json"

    def from(value: JValue): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      write(value, new OutputStreamWriter(baos, charset)).close()
      baos.toByteArray
    }
  }

  implicit object JsonRespondable extends ResponseHandler[JsonResponse] {
    def to(value: Response): JsonResponse = JsonResponse(value.getStatusCode, as.String(value))
  }

  implicit object JsonDecomposer extends Decomposer[JValue] {
    def decompose[Value](value: Value): JValue = {
      net.liftweb.json.Extraction.decompose(value)
    }
  }

}

case class JsonResponse(code: Int, str: String) extends ConvertableResponse[TypedJsonResponse] {
  val json: Option[JValue] = Try(JsonParser.parse(str)).toOption

  def as[A](implicit mf: Manifest[A], formats: Formats) = TypedJsonResponse[A](code, str, json)
}

case class TypedJsonResponse[A](code: Int, str: String, json: Option[JValue])
                               (implicit mf: Manifest[A], formats: Formats) {
  val value: Option[A] = {
    if (code >= 200 && code != 204 && (code < 300 || code == 409)) {
      json.flatMap(j => Try(j.extract[A]).toOption)
    } else None
  }
}
