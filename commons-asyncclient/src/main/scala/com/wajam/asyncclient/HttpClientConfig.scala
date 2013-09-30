package com.wajam.asyncclient

trait BaseHttpClientConfig {
  def allowPoolingConnection: Boolean
  def connectionTimeoutInMs: Int
  def requestTimeoutInMs: Int
  def maximumConnectionsPerHost: Int
  def maximumConnectionsTotal: Int
  def threadPoolSize: Int
}

object BaseHttpClientConfig {
  val unlimited = -1
}

case class HttpClientConfig(allowPoolingConnection: Boolean = false,
                            connectionTimeoutInMs: Int = 1000,
                            requestTimeoutInMs: Int = 1000,
                            maximumConnectionsPerHost: Int = BaseHttpClientConfig.unlimited,
                            maximumConnectionsTotal: Int = BaseHttpClientConfig.unlimited,
                            threadPoolSize: Int = 1) extends BaseHttpClientConfig
