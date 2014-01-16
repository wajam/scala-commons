package com.wajam.asyncclient

import scala.concurrent.{ExecutionContext, Future}
import scala.language.higherKinds
import com.wajam.tracing.{TracedTimer, Traced}

trait ResourceModule[RequestBody, ResponseMessage <: ConvertableResponse[TypedResponse], TypedResponse[_]] extends Traced {

  protected def client: BaseAsyncClient

  implicit protected def requestHandler: RequestHandler[RequestBody]

  implicit protected def responseHandler: ResponseHandler[ResponseMessage]

  implicit protected def decomposer: Decomposer[RequestBody]

  private def timeAction[T](timer: TracedTimer)(action: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val context = timer.timerContext()
    val actionFuture = action
    actionFuture onComplete {
      case _ => context.stop()
    }
    actionFuture
  }

  trait Resource {
    protected def url: String

    protected def name: String
  }

  trait CreatableResource[Value] extends Resource {
    lazy val createMeter = tracedTimer(s"$name-creates")

    def create(value: Value, params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      timeAction(createMeter) {
        client.post(AsyncClient.url(url).params(params), decomposer.decompose(value)).map(_.as[Value])
      }
    }
  }

  trait ApplicableResource[Key, R <: Resource] extends Resource {
    def apply(key: Key): R
  }

  trait GettableResource[Value] extends Resource {
    lazy val getMeter = tracedTimer(s"$name-gets")

    def get(params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      timeAction(getMeter) {
        client.get(AsyncClient.url(url).params(params)).map(_.as[Value])
      }
    }
  }

  trait ListableResource[Value] extends Resource {
    lazy val listMeter = tracedTimer(s"$name-lists")

    def list(params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      timeAction(listMeter) {
        client.get(AsyncClient.url(url).params(params)).map(_.as[Value])
      }
    }
  }

  trait UpdatableResource[Value] extends Resource {
    lazy val updateMeter = tracedTimer(s"$name-updates")

    def update(value: Value, params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      timeAction(updateMeter) {
        client.put(AsyncClient.url(url).params(params), decomposer.decompose(value)).map(_.as[Value])
      }
    }
  }

  trait DeletableResource[Value] extends Resource {
    lazy val deleteMeter = tracedTimer(s"$name-deletes")

    def delete(params: Map[String, String] = Map())(implicit mf: Manifest[Value], ec: ExecutionContext): Future[TypedResponse[Value]] = {
      timeAction(deleteMeter) {
        client.delete(AsyncClient.url(url).params(params)).map(_.as[Value])
      }
    }
  }

}

trait Decomposer[I] {
  def decompose[Value](value: Value): I
}

