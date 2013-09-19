package com.wajam.asyncclient

import net.liftweb.json.JsonAST.JValue
import net.liftweb.json.Formats

trait ResourceModule {

  protected def client: JsonClient

  implicit protected val formats: Formats
  implicit protected def ec = client.ec

  private def fromValue[Value](value: Value): JValue = {
    import net.liftweb.json.Extraction.decompose
    decompose(value)
  }

  trait Resource {
    protected def url: String
  }

  trait PostableResource[Value] {
    this: Resource =>

    def create(value: Value)(implicit mf: Manifest[Value]): AsyncJsonResponse[Value] = {
      client.post(url, fromValue(value)).map(_.as[Value])
    }
  }

  trait ApplicableResource[Key, R <: Resource] {
    this: Resource =>

    def apply(key: Key): R
  }

  trait GettableResource[Value] {
    this: Resource =>

    def get()(implicit mf: Manifest[Value]): AsyncJsonResponse[Value] = {
      client.get(url).map(_.as[Value])
    }
  }

  trait UpdatableResource[Value] {
    this: Resource =>

    def update(value: Value)(implicit mf: Manifest[Value]): AsyncJsonResponse[Value] = {
      client.put(url, fromValue(value)).map(_.as[Value])
    }
  }

  trait DeletableResource[Value] {
    this: Resource =>

    def delete()(implicit mf: Manifest[Value]): AsyncJsonResponse[Value] = {
      client.delete(url).map(_.as[Value])
    }
  }

}
