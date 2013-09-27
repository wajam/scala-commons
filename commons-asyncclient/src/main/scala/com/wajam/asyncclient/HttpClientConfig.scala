package com.wajam.asyncclient

trait BaseHttpClientConfig {
  def allowPoolingConnection: Boolean
  def connectionTimeoutInMs: Int
  def requestTimeoutInMs: Int
  def maximumConnectionsPerHost: Int
  def maximumConnectionsTotal: Int
  def threadPoolSize: Int
}

case class HttpClientConfig(allowPoolingConnection: Boolean = false,
                            connectionTimeoutInMs: Int = 1000,
                            requestTimeoutInMs: Int = 1000,
                            maximumConnectionsPerHost: Int = -1,
                            maximumConnectionsTotal: Int = -1,
                            threadPoolSize: Int = 1) extends BaseHttpClientConfig
