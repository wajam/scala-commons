package com.wajam.asyncclient

trait AsyncClientTest {

  val testConfig = HttpClientConfig(
    connectionTimeoutInMs = 100,
    requestTimeoutInMs = 5000,
    threadPoolSize = 1)
  val testUrl = "http://www.wajam.com"
  val testString = "Wajam"

  lazy val saxParserFactory = {
    val spf = new org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl()
    spf.setNamespaceAware(true)
    spf
  }
}
