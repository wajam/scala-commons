package com.wajam.asyncclient

import scala.concurrent.{ ExecutionContext, Future }
import scala.language.higherKinds
import com.wajam.tracing.{ TracedTimer, Traced }
import scala.util.Success
import com.ning.http.client.Response

trait ResourceModule[RequestBody, ResponseMessage <: ConvertableResponse[TypedResponse], TypedResponse[_]] extends Traced {

  protected def client: BaseAsyncClient

  implicit protected def requestHandler: RequestHandler[RequestBody]

  implicit protected def responseHandler: ResponseHandler[ResponseMessage]

  implicit protected def decomposer: Decomposer[RequestBody]

  private def timeAction[T](timer: TracedTimer)(action: => Future[(Map[String, String], T)])(implicit ec: ExecutionContext): Future[T] = {
    val context = timer.timerContext()
    val actionFuture = action
    actionFuture onComplete {
      case Success((extra, _)) => context.stop(extra)
      case _ => context.stop()
    }
    actionFuture.map { case (_, value) => value }
  }

  trait Resource {
    protected def url: String

    protected def name: String
  }

  trait CreatableResource[Value] extends Resource {
    lazy val createMeter = tracedTimer(s"$name-creates")

    def create(value: Value, params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      implicit val captor = new ResponseHandlerCaptor
      timeAction(createMeter) {
        val responseFuture = client.post(AsyncClient.url(url).params(params), decomposer.decompose(value))
        responseFuture.map(response => (captor.extra, response.as[Value]))
      }
    }
  }

  trait ApplicableResource[Key, R <: Resource] extends Resource {
    def apply(key: Key): R
  }

  trait GettableResource[Value] extends Resource {
    lazy val getMeter = tracedTimer(s"$name-gets")

    def get(params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      implicit val captor = new ResponseHandlerCaptor
      timeAction(getMeter) {
        val responseFuture = client.get(AsyncClient.url(url).params(params))
        responseFuture.map(response => (captor.extra, response.as[Value]))
      }
    }
  }

  trait ListableResource[Value] extends Resource {
    lazy val listMeter = tracedTimer(s"$name-lists")

    def list(params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      implicit val captor = new ResponseHandlerCaptor
      timeAction(listMeter) {
        val responseFuture = client.get(AsyncClient.url(url).params(params))
        responseFuture.map(response => (captor.extra, response.as[Value]))
      }
    }
  }

  trait UpdatableResource[Value] extends Resource {
    lazy val updateMeter = tracedTimer(s"$name-updates")

    def update(value: Value, params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      implicit val captor = new ResponseHandlerCaptor
      timeAction(updateMeter) {
        val responseFuture = client.put(AsyncClient.url(url).params(params), decomposer.decompose(value))
        responseFuture.map(response => (captor.extra, response.as[Value]))
      }
    }
  }

  trait DeletableResource[Value] extends Resource {
    lazy val deleteMeter = tracedTimer(s"$name-deletes")

    def delete(params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      implicit val captor = new ResponseHandlerCaptor
      timeAction(deleteMeter) {
        val responseFuture = client.delete(AsyncClient.url(url).params(params))
        responseFuture.map(response => (captor.extra, response.as[Value]))
      }
    }
  }

  private class ResponseHandlerCaptor extends ResponseHandler[ResponseMessage] {
    private var response: Option[Response] = None

    def to(value: Response) = {
      response = Some(value)
      responseHandler.to(value)
    }

    def extra: Map[String, String] = response.map(r => Map("code" -> r.getStatusCode.toString)).getOrElse(Map())
  }
}

trait Decomposer[I] {
  def decompose[Value](value: Value): I
}

