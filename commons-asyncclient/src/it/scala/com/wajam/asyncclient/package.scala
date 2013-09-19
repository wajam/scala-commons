package com.wajam

package object asyncclient {

  val testConfig = HttpClientConfig(
    connectionTimeoutInMs = 100,
    requestTimeoutInMs = 2000,
    threadPoolSize = 2)
  val testUrl = "http://www.wajam.com"
  val testString = "Wajam"

}
