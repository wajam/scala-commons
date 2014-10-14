package com.wajam.asyncclient

import com.ning.http.client.Response
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, FunSuite }
import org.mockito.Mockito._

class TestAsyncClient extends FunSuite with Matchers with MockitoSugar {

  test("should extract charset from Content-Type string") {

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
