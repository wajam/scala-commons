package com.wajam.asyncclient

import net.liftweb.json.Formats
import scala.concurrent.Future
import net.liftweb.json.JsonAST.JValue
import scala.language.higherKinds

trait ResourceModule[I, TypedResponse[_], Inner <: ConvertableResponse[TypedResponse]] {

  protected def client: AsyncClient

  implicit protected def requestHandler: RequestHandler[I]
  implicit protected def responseHandler: ResponseHandler[Inner]
  implicit protected def decomposer: Decomposer[I]
  implicit protected def formats: Formats

  trait Resource {
    protected def url: String
  }

  trait CreatableResource[Value] extends Resource {
    def create(value: Value)(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      client.post[I, Inner, TypedResponse[Value]](url, decomposer.decompose(value))(_.as[Value])
    }
  }

  trait ApplicableResource[Key, R <: Resource] extends Resource {
    def apply(key: Key): R
  }

  trait GettableResource[Value] extends Resource {
    def get()(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      client.get[Inner, TypedResponse[Value]](url)(_.as[Value])
    }
  }

  trait UpdatableResource[Value] extends Resource {
    def update(value: Value)(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      client.put[I, Inner, TypedResponse[Value]](url, decomposer.decompose(value))(_.as[Value])
    }
  }

  trait DeletableResource[Value] extends Resource {
    def delete()(implicit mf: Manifest[Value]): Future[TypedResponse[Value]] = {
      client.delete[Inner, TypedResponse[Value]](url)(_.as[Value])
    }
  }

}

trait Decomposer[I] {
  def decompose[Value](value: Value): I
}

abstract class JsonResourceModule extends ResourceModule[JValue, TypedJsonResponse, JsonResponse] with JsonOperations {
  implicit protected def requestHandler: RequestHandler[JValue] = JsonRequestable

  implicit protected def responseHandler: ResponseHandler[JsonResponse] = JsonRespondable

  implicit protected def decomposer: Decomposer[JValue] = JsonDecomposer
}
