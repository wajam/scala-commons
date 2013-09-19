package com.wajam.asyncclient

import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.Serialization.write
import com.ning.http.client.Response
import dispatch.as
import java.io.{OutputStreamWriter, ByteArrayOutputStream}
import net.liftweb.json.{JsonParser, Formats}
import scala.util.Try

class JsonClient(protected val config: BaseHttpClientConfig, charset: String)
                (implicit formats: Formats) extends BaseClient[JValue, JsonResponse] {
  protected val contentType = "application/json"

  protected def to(value: Response): JsonResponse = JsonResponse(value.getStatusCode, as.String(value))

  protected def from(value: JValue): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    write(value, new OutputStreamWriter(baos, charset)).close()
    baos.toByteArray
  }
}

case class JsonResponse(code: Int, str: String) {
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
