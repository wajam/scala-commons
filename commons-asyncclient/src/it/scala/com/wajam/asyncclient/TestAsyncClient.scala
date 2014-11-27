package com.wajam.asyncclient

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import com.ning.http.client.Response
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }

import com.wajam.asyncclient.AsyncClient._

class TestAsyncClient extends FlatSpec with Matchers {

  "AsyncClient" should "be able to get content from a website" in new AsyncClientTest {
    implicit object HtmlResponse extends ResponseHandler[xml.Elem] {
      // See https://github.com/dispatch/reboot/blob/master/core/src/main/scala/as/xml/elem.scala
      //Also, there is a bug with the doctype
      def to(response: Response) = xml.XML.withSAXParser(saxParserFactory.newSAXParser).loadString(response.getResponseBody)
    }
    val client = new AsyncClient(testConfig)
    extractTitle(Await.result(client.get(url(testUrl)), 10.seconds)) should include(testString)
  }

  private def extractTitle(x: xml.Elem): String = (x \ "head" \ "title").text

  it should "extract charset from Content-Type string" in new MockitoSugar {

    val response = mock[Response]

    val defaultCharset = "UTF-8"

    def innerTest(contentType: String, charset: String): Unit = {
      when(response.getContentType).thenReturn(contentType)
      AsyncClient.extractCharset(response, defaultCharset) should be(charset)
    }

    innerTest("text/xml; charset=UTF-8", "UTF-8")
    innerTest("text/xml; charset=ISO-8859-1", "ISO-8859-1")
    innerTest("text/xml; charset=", defaultCharset)
    innerTest("text/xml; charset=", defaultCharset)
    innerTest("charset=", defaultCharset)
    innerTest("charset=ISO-8859-1", "ISO-8859-1")
    innerTest("charsdet=ISO-8859-1", defaultCharset)
  }
}
