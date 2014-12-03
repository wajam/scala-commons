package com.wajam.commons.elasticsearch

import scala.collection.JavaConversions._
import scala.concurrent.{ ExecutionContext, Future, Promise }

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.cluster.health.{ ClusterHealthResponse, ClusterHealthStatus }
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse
import org.elasticsearch.action.admin.indices.create.{ CreateIndexRequestBuilder, CreateIndexResponse }
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.QueryBuilder

class AsyncElasticsearchClient(val client: Client) {

  def indexObject[T <: AnyRef](index: String, value: T, overrideId: Option[String] = None, overrideType: Option[String] = None)(implicit ec: ExecutionContext, ser: ElasticsearchSerializer): Future[IndexResponse] = {
    wrapFuture[IndexResponse] {
      client.prepareIndex(index, overrideType.getOrElse(getTypeName(value)), overrideId.orNull)
        .setSource(ser.serialize[T](value))
        .execute
    }
  }

  def searchObject[T](indices: Iterable[String], queryBuilder: QueryBuilder)(implicit ec: ExecutionContext, ser: ElasticsearchSerializer, mf: Manifest[T]): Future[Iterator[T]] = {
    search(indices, queryBuilder, Seq(getTypeName(mf))).map {
      _.getHits.iterator.toIterator
        .map(f => ser.deserialize[T](f.getSourceAsString.getBytes(ser.charset)))
    }
  }

  def search(indices: Iterable[String], queryBuilder: QueryBuilder, types: Iterable[String] = Nil)(implicit ec: ExecutionContext): Future[SearchResponse] = {
    wrapFuture[SearchResponse] {
      client.prepareSearch(indices.toSeq: _*)
        .setQuery(queryBuilder)
        .setTypes(types.toSeq: _*)
        .execute
    }
  }

  def getObject[T <: AnyRef](index: String, id: String)(implicit ec: ExecutionContext, ser: ElasticsearchSerializer, mf: Manifest[T]): Future[Option[T]] = {
    get(index, id, Some(getTypeName(mf))).map {
      case r if r.isExists => Some(ser.deserialize[T](r.getSourceAsBytes))
      case _ => None
    }
  }

  def get(index: String, id: String, typ: Option[String] = None)(implicit ec: ExecutionContext): Future[GetResponse] = {
    wrapFuture[GetResponse] {
      client.prepareGet(index, typ.orNull, id)
        .execute
    }
  }

  def deleteObject[T <: AnyRef](index: String, id: String, overrideType: Option[String] = None)(implicit ec: ExecutionContext, mf: Manifest[T]): Future[DeleteResponse] = {
    delete(index, id, overrideType orElse Some(getTypeName(mf)))
  }

  def delete(index: String, id: String, typ: Option[String] = None)(implicit ec: ExecutionContext): Future[DeleteResponse] = {
    wrapFuture[DeleteResponse] {
      client.prepareDelete(index, typ.orNull, id)
        .execute
    }
  }

  object admin {

    /**
     * A specialized simplified mapping source method, takes the form of simple properties definition:
     * ("field1", "type=string,store=true")
     */
    type FieldMapping = Map[String, String]

    private def adminClient = client.admin

    def getClusterHealth(indices: Iterable[String], waitForStatus: Option[ClusterHealthStatus] = None)(implicit ec: ExecutionContext): Future[ClusterHealthResponse] = {
      wrapFuture[ClusterHealthResponse] {
        adminClient.cluster.prepareHealth(indices.toSeq: _*)
          .setWaitForStatus(waitForStatus.orNull)
          .execute
      }
    }

    def getIndexRecovery(index: String, activeOnly: Boolean = false, detailed: Boolean = false)(implicit ec: ExecutionContext): Future[RecoveryResponse] = getIndicesRecoveries(Seq(index), activeOnly, detailed)

    def getIndicesRecoveries(indices: Iterable[String], activeOnly: Boolean = false, detailed: Boolean = false)(implicit ec: ExecutionContext): Future[RecoveryResponse] = {
      wrapFuture[RecoveryResponse] {
        adminClient.indices.prepareRecoveries(indices.toSeq: _*)
          .setDetailed(detailed)
          .setActiveOnly(activeOnly)
          .execute
      }
    }

    def openIndex(index: String)(implicit ec: ExecutionContext): Future[OpenIndexResponse] = openIndices(Seq(index))

    def openIndices(indices: Iterable[String])(implicit ec: ExecutionContext): Future[OpenIndexResponse] = {
      wrapFuture[OpenIndexResponse] {
        adminClient.indices.prepareOpen(indices.toSeq: _*)
          .execute
      }
    }

    def closeIndex(index: String)(implicit ec: ExecutionContext): Future[CloseIndexResponse] = closeIndices(Seq(index))

    def closeIndices(indices: Iterable[String])(implicit ec: ExecutionContext): Future[CloseIndexResponse] = {
      wrapFuture[CloseIndexResponse] {
        adminClient.indices.prepareClose(indices.toSeq: _*)
          .execute
      }
    }

    def refreshIndex(index: String)(implicit ec: ExecutionContext): Future[RefreshResponse] = refreshIndices(Seq(index))

    def refreshIndices(indices: Iterable[String])(implicit ec: ExecutionContext): Future[RefreshResponse] = {
      wrapFuture[RefreshResponse] {
        adminClient.indices.prepareRefresh(indices.toSeq: _*)
          .execute
      }
    }

    def existsIndex(index: String)(implicit ec: ExecutionContext): Future[IndicesExistsResponse] = {
      wrapFuture[IndicesExistsResponse] {
        adminClient.indices.prepareExists(index)
          .execute
      }
    }

    def createIndex(index: String, typeMapping: Map[String, FieldMapping] = Map())(implicit ec: ExecutionContext): Future[CreateIndexResponse] = {
      wrapFuture[CreateIndexResponse] {
        val builder = adminClient.indices.prepareCreate(index)
        typeMapping.foldLeft[CreateIndexRequestBuilder](builder) {
          case (b, (typ, mapping)) =>
            b.addMapping(typ, mapping.flatMap { case (k, v) => Seq(k, v) }.toSeq: _*)
        }.execute
      }
    }

    def deleteIndex(index: String)(implicit ec: ExecutionContext): Future[DeleteIndexResponse] = deleteIndices(Seq(index))

    def deleteIndices(indices: Iterable[String])(implicit ec: ExecutionContext): Future[DeleteIndexResponse] = {
      wrapFuture[DeleteIndexResponse] {
        adminClient.indices.prepareDelete(indices.toSeq: _*)
          .execute
      }
    }

    def getMapping(index: String, typ: String)(implicit ec: ExecutionContext): Future[GetMappingsResponse] = getMapping(Seq(index), Seq(typ))

    def getMapping(indices: Iterable[String], types: Iterable[String])(implicit ec: ExecutionContext): Future[GetMappingsResponse] = {
      wrapFuture[GetMappingsResponse] {
        adminClient.indices.prepareGetMappings(indices.toSeq: _*)
          .setTypes(types.toSeq: _*)
          .execute
      }
    }

    def putMapping(index: String, typ: String, mapping: FieldMapping)(implicit ec: ExecutionContext): Future[PutMappingResponse] = putMapping(Seq(index), typ, mapping)

    def putMapping(indices: Iterable[String], typ: String, mapping: FieldMapping)(implicit ec: ExecutionContext): Future[PutMappingResponse] = {
      wrapFuture[PutMappingResponse] {
        adminClient.indices.preparePutMapping(indices.toSeq: _*)
          .setType(typ)
          .setSource(mapping.flatMap { case (k, v) => Seq(k, v) }.toSeq: _*)
          .execute
      }
    }

  }

  private def getTypeName[T](value: T): String = {
    value.getClass.getSimpleName.toLowerCase
  }

  private def getTypeName(mf: Manifest[_]): String = {
    mf.runtimeClass.getSimpleName.toLowerCase
  }

  private def wrapFuture[A](f: ActionListener[A] => Unit)(implicit ec: ExecutionContext): Future[A] = {
    val p = Promise[A]()
    f(new ActionListener[A] {
      def onFailure(e: Throwable): Unit = p.tryFailure(e)

      def onResponse(response: A): Unit = p.trySuccess(response)
    })
    p.future
  }

}
