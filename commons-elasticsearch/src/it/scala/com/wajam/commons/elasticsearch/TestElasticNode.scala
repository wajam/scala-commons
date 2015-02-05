package com.wajam.commons.elasticsearch

import java.io.File
import java.util.UUID

import scala.concurrent.ExecutionContext

import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.node.NodeBuilder._

class TestClient {

  val client = TestElasticNode.client

}

object TestElasticNode {

  implicit val serializer = new ElasticsearchJsonSerializer

  val tempFile = File.createTempFile("elasticsearchtests", "tmp")
  val homeDir = new File(tempFile.getParent + "/" + UUID.randomUUID().toString)
  homeDir.mkdir()
  homeDir.deleteOnExit()
  tempFile.deleteOnExit()

  val settings = ImmutableSettings.settingsBuilder()
    .put("node.http.enabled", false)
    .put("http.enabled", false)
    .put("path.home", homeDir.getAbsolutePath)
    .put("index.number_of_shards", 1)
    .put("index.number_of_replicas", 0)
    .put("script.disable_dynamic", false)
    .put("es.logger.level", "DEBUG")

  private val nodeClient = nodeBuilder()
    .settings(settings)
    .local(true)
    .node()
    .client()

  val client = new AsyncElasticsearchClient(nodeClient)

  def reset()(implicit ec: ExecutionContext) = {
    client.admin.deleteIndex("_all")
  }

  def close() = {
    nodeClient.close()
  }

}
