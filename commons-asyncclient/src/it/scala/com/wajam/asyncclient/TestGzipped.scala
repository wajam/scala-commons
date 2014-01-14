package com.wajam.asyncclient

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.{Matchers, FlatSpec}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.JsonAST.{JObject, JBool, JField}

class TestGzipped extends FlatSpec with Matchers with JsonOperations {

  "AsyncClient" should "handle gzipped responses" in {
    val c = new AsyncClient(HttpClientConfig(compressionEnabled = true))
    val r = Await.result(c.get(AsyncClient.url("http://httpbin.org/gzip")), Duration.Inf)

    r.json match {
      case Some(j) => {
        for {
          JObject(json) <- j
          JField("gzipped", JBool(gzipped)) <- json
        } gzipped should be(true)
      }
      case None => fail(s"json undefined: ${r.str}")
    }
  }

  protected def charset: String = "utf-8"

  implicit protected def formats: Formats = DefaultFormats
}
