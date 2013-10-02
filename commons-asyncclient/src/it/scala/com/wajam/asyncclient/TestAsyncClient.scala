package com.wajam.asyncclient

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import com.ning.http.client.Response
import scala.concurrent.Await
import scala.concurrent.duration._
import com.wajam.asyncclient.AsyncClient.stringToReq

class TestAsyncClient extends FunSuite with ShouldMatchers {

  test("should be able to get content from a website") {
    implicit object HtmlResponse extends ResponseHandler[xml.Elem] {
      // See https://github.com/dispatch/reboot/blob/master/core/src/main/scala/as/xml/elem.scala
      //Also, there is a bug with the doctype
      def to(response: Response) = xml.XML.withSAXParser(factory.newSAXParser).loadString(response.getResponseBody.dropWhile(_ != '\n'))

      private lazy val factory = {
        val spf = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl()
        spf.setNamespaceAware(true)
        spf
      }
    }
    val client = new AsyncClient(HttpClientConfig())
    extractTitle(Await.result(client.get("http://www.wajam.com"), 10.seconds)) should include("Wajam")
  }

  private def extractTitle(x: xml.Elem): String = (x \ "head" \ "title").text

}
