package com.wajam.asyncclient

import scala.concurrent.Future
import scala.language.higherKinds

trait ResourceModule[RequestBody, ResponseMessage <: ConvertableResponse[TypedResponse], TypedResponse[_]] {

  protected def client: AsyncClient

  implicit protected def requestHandler: RequestHandler[RequestBody]

  implicit protected def responseHandler: ResponseHandler[ResponseMessage]

  implicit protected def decomposer: Decomposer[RequestBody]

  trait Resource {
    protected def url: String
  }

  trait CreatableResource[Value] extends Resource {
    def create(value: Value)(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      client.post[RequestBody, ResponseMessage, TypedResponse[Value]](url, decomposer.decompose(value))(_.as[Value])
    }
  }

  trait ApplicableResource[Key, R <: Resource] extends Resource {
    def apply(key: Key): R
  }

  trait GettableResource[Value] extends Resource {
    def get()(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      client.get[ResponseMessage, TypedResponse[Value]](url)(_.as[Value])
    }
  }

  trait UpdatableResource[Value] extends Resource {
    def update(value: Value)(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      client.put[RequestBody, ResponseMessage, TypedResponse[Value]](url, decomposer.decompose(value))(_.as[Value])
    }
  }

  trait DeletableResource[Value] extends Resource {
    def delete()(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      client.delete[ResponseMessage, TypedResponse[Value]](url)(_.as[Value])
    }
  }

}

trait Decomposer[I] {
  def decompose[Value](value: Value): I
}

