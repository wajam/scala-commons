package com.wajam.asyncclient

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

import org.json4s.JsonAST.{ JBool, JField, JObject }
import org.json4s.{ DefaultFormats, Formats }
import org.scalatest.{ FlatSpec, Matchers }

class TestGzipped extends FlatSpec with Matchers with JsonOperations {

  "AsyncClient" should "handle gzipped responses" in {
    val c = new AsyncClient(HttpClientConfig(compressionEnabled = true))
    val r = Await.result(c.get(AsyncClient.url("http://httpbin.org/gzip")), Duration.Inf)

    for {
      JObject(json) <- r.json.get
      JField("gzipped", JBool(gzipped)) <- json
    } gzipped should be(true)
  }

  protected def charset: String = "utf-8"

  implicit protected def formats: Formats = DefaultFormats
}
