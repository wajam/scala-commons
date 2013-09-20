package com.wajam.asyncclient

import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.Formats

trait ResourceModule {

  protected def client: JsonClient

  implicit protected val formats: Formats

  private def fromValue[Value](value: Value): JValue = {
    import net.liftweb.json.Extraction.decompose
    decompose(value)
  }

  trait Resource {
    protected def url: String
  }

  trait CreatableResource[Value] extends Resource {
    def create(value: Value)(implicit mf: Manifest[Value]): AsyncJsonResponse[Value] = {
      client.post(url, fromValue(value))(_.as[Value])
    }
  }

  trait ApplicableResource[Key, R <: Resource] extends Resource {
    def apply(key: Key): R
  }

  trait GettableResource[Value] extends Resource {
    def get()(implicit mf: Manifest[Value]): AsyncJsonResponse[Value] = {
      client.get(url)(_.as[Value])
    }
  }

  trait UpdatableResource[Value] extends Resource {
    def update(value: Value)(implicit mf: Manifest[Value]): AsyncJsonResponse[Value] = {
      client.put(url, fromValue(value))(_.as[Value])
    }
  }

  trait DeletableResource[Value] extends Resource {
    def delete()(implicit mf: Manifest[Value]): AsyncJsonResponse[Value] = {
      client.delete(url)(_.as[Value])
    }
  }

}
