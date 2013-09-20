package com.wajam.asyncclient

import scala.concurrent.{ExecutionContext, Future}
import dispatch._
import com.ning.http.client
import java.util.concurrent.Executors

trait BaseClient[Request, Response] {

  private[asyncclient] implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(config.threadPoolSize))

  protected def config: BaseHttpClientConfig

  protected def contentType: String

  protected val httpClient = Http.configure(_.
    setAllowPoolingConnection(config.allowPoolingConnection).
    setConnectionTimeoutInMs(config.connectionTimeoutInMs).
    setRequestTimeoutInMs(config.requestTimeoutInMs).
    setMaximumConnectionsPerHost(config.maximumConnectionsPerHost).
    setMaximumConnectionsTotal(config.maximumConnectionsTotal)
  )

  private def setBody(req: Req, value: Request): Req = {
    req.setHeader("content-type", contentType).setBody(from(value))
  }

  def get(myUrl: String): Future[Response] = {
    httpClient(url(myUrl) > to _)
  }

  def post(myUrl: String, value: Request): Future[Response] = {
    httpClient(setBody(url(myUrl).POST, value) > to _)
  }

  def put(myUrl: String, value: Request): Future[Response] = {
    httpClient(setBody(url(myUrl).PUT, value) > to _)
  }

  def delete(myUrl: String): Future[Response] = {
    httpClient(url(myUrl).DELETE > to _)
  }

  protected def to(value: client.Response): Response

  protected def from(value: Request): Array[Byte]

}
