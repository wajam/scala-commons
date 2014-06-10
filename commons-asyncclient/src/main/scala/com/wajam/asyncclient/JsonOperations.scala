package com.wajam.asyncclient

import org.json4s.JsonAST.JValue
import org.json4s.native.Serialization.write
import com.ning.http.client.Response
import dispatch.as
import java.io.{ OutputStreamWriter, ByteArrayOutputStream }
import org.json4s.{Extraction, Formats}
import org.json4s.native.JsonMethods._
import scala.util.Try
import scala.collection.Iterable

trait JsonOperations {
  jsonOperations =>

  protected def charset: String
  implicit protected def formats: Formats

  implicit object JsonRequestHandler extends RequestHandler[JValue] {
    val contentType = "application/json"
    val charset = Some(jsonOperations.charset)

    def from(value: JValue): Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      write(value, new OutputStreamWriter(baos, jsonOperations.charset)).close()
      baos.toByteArray
    }
  }

  implicit object JsonResponseHandler extends ResponseHandler[JsonResponse] {
    def to(value: Response): JsonResponse = JsonResponse(value.getStatusCode, as.String(value), Headers(value))
  }

  implicit object JsonDecomposer extends Decomposer[JValue] {
    def decompose[Value](value: Value): JValue = {
      Extraction.decompose(value)
    }
  }

}

case class JsonResponse(code: Int, json: Option[JValue], headers: Headers)(implicit formats: Formats) extends ConvertableResponse[TypedJsonResponse] {
  def as[A](implicit mf: Manifest[A]) = TypedJsonResponse[A](code, json, headers)
}

object JsonResponse {
  def apply(code: Int, str: String, headers: Headers = Headers.Empty)(implicit formats: Formats): JsonResponse = {
    val json: Option[JValue] = Try(parse(str)).toOption
    new JsonResponse(code, json, headers)
  }
}

case class TypedJsonResponse[A](code: Int, value: Option[A], headers: Headers)

object TypedJsonResponse {
  def apply[A](code: Int, json: Option[JValue], headers: Headers)(implicit mf: Manifest[A], formats: Formats): TypedJsonResponse[A] = {
    val value: Option[A] = {
      if (code >= 200 && code != 204 && (code < 300 || code == 409)) {
        json.flatMap(j => Try(j.extract[A]).toOption)
      } else None
    }

    new TypedJsonResponse(code, value, headers)
  }
}

trait JsonResourceModule extends ResourceModule[JValue, JsonResponse, TypedJsonResponse] with JsonOperations {
  implicit protected def requestHandler: RequestHandler[JValue] = JsonRequestHandler

  implicit protected def responseHandler: ResponseHandler[JsonResponse] = JsonResponseHandler

  implicit protected def decomposer: Decomposer[JValue] = JsonDecomposer
}
