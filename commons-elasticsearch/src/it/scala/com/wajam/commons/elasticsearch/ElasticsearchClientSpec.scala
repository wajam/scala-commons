package com.wajam.commons.elasticsearch

import java.util
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext

import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
import org.elasticsearch.index.query.{ MatchAllQueryBuilder, TermQueryBuilder }
import org.scalatest.Matchers._
import org.scalatest.concurrent.{ IntegrationPatience, ScalaFutures }
import org.scalatest.time.{ Span, _ }
import org.scalatest.{ BeforeAndAfter, BeforeAndAfterAll, FlatSpec }

import com.wajam.commons.elasticsearch.Twitter.Tweet

class ElasticsearchClientSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures with IntegrationPatience {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val ser = new ElasticsearchJsonSerializer

  trait Setup {
    val client = new TestClient().client
  }

  before {
    TestElasticNode.reset()
  }

  after {
    TestElasticNode.close()
  }

  "Elasticsearch Client" should "index (any) object" in new Setup {
    val idx = for {
      _ <- client.admin.createIndex("twitter")
      i <- client.indexObject("twitter", Tweet(1, "First!"))
    } yield i

    whenReady(idx) { v =>
      v should be('Created)
      v.getIndex should be("twitter")
      v.getType should be("tweet")
    }
  }

  it should "index (any) object with defined mapping" in new Setup {
    val mapping = Map("tweet" -> Map("id" -> "type=long,store=true", "message" -> "type=string,store=true"))
    val idx = for {
      _ <- client.admin.createIndex("twitter", mapping)
      i <- client.indexObject("twitter", Tweet(1, "First!"))
    } yield i

    whenReady(idx) { v =>
      v should be('Created)
      v.getIndex should be("twitter")
      v.getType should be("tweet")
    }
  }

  it should "get (any) indexed object" in new Setup {
    val tweet = Tweet(1, "First!")
    val get = for {
      _ <- client.admin.createIndex("twitter")
      i <- client.indexObject("twitter", tweet)
      g <- client.getObject[Tweet]("twitter", i.getId)
    } yield g

    whenReady(get) { v =>
      v should be('Defined)
      v should equal(Some(tweet))
    }
  }

  it should "search basic indexed objects" in new Setup {
    val tweet1 = Tweet(1, "First tweet !")
    val tweet2 = Tweet(2, "Second post !")
    val tweet3 = Tweet(3, "Third tweet !")

    val qb = new TermQueryBuilder("message", "tweet")

    val search = for {
      _ <- client.admin.createIndex("twitter")
      i1 <- client.indexObject("twitter", tweet1)
      i2 <- client.indexObject("twitter", tweet2)
      i3 <- client.indexObject("twitter", tweet3)
      _ <- client.admin.refreshIndex("twitter") // refresh to make document available
      s <- client.searchObject[Tweet](Seq("twitter"), qb)
    } yield s

    whenReady(search) { v =>
      val results = v.toStream

      // Basic search test
      results should have size 2
      results should contain(tweet1)
      results should contain(tweet3)
    }
  }

  it should "delete indexed object" in new Setup {
    val tweet = Tweet(1, "First!")
    val get = for {
      _ <- client.admin.createIndex("twitter")
      i <- client.indexObject("twitter", tweet)
      g1 <- client.getObject[Tweet]("twitter", i.getId)
      d <- client.deleteObject[Tweet]("twitter", i.getId)
      g2 <- client.getObject[Tweet]("twitter", i.getId)
    } yield (g1, d, g2)

    whenReady(get) {
      case (v, d, n) =>
        v should be('Defined)
        d should be('Found)
        n should not be ('Defined)
    }
  }

  "Admin Client" should "create index without mapping" in new Setup {
    val exists = for {
      e1 <- client.admin.existsIndex("twitter")
      c <- client.admin.createIndex("twitter")
      e2 <- client.admin.existsIndex("twitter")
    } yield (e1, e2)

    whenReady(exists) {
      case (v1, v2) =>
        v1 should not be ('Exists)
        v2 should be('Exists)
    }
  }

  it should "create index with mapping" in new Setup {
    val mapping = Map("tweet" -> Map("id" -> "type=long,store=true", "message" -> "type=string,store=true"))
    val exists = for {
      _ <- client.admin.createIndex("twitter", mapping)
      e <- client.admin.existsIndex("twitter")
    } yield e

    whenReady(exists) { v =>
      v should be('Exists)
    }

    whenReady(client.admin.getMapping("twitter", "tweet")) { v =>
      val indexMappings = v.mappings.get("twitter")
      indexMappings should not be null

      val tweetMapping = indexMappings.get("tweet")
      tweetMapping should not be null
    }
  }

  it should "delete index" in new Setup {
    val exists = for {
      _ <- client.admin.createIndex("twitter")
      e1 <- client.admin.existsIndex("twitter")
      d <- client.admin.deleteIndex("twitter")
      e2 <- client.admin.existsIndex("twitter")
    } yield (e1, e2)

    whenReady(exists) {
      case (v1, v2) =>
        v1 should be('Exists)
        v2 should not be ('Exists)
    }
  }

  it should "close index" in new Setup {
    val close = for {
      _ <- client.admin.createIndex("twitter")
      _ <- client.admin.getClusterHealth(Seq("twitter"), waitForStatus = Some(ClusterHealthStatus.YELLOW))
      ci <- client.admin.closeIndex("twitter")
    } yield ci

    whenReady(close) { v =>
      v should be('Acknowledged)
    }
  }

  it should "open index" in new Setup {
    val open = for {
      _ <- client.admin.createIndex("twitter")
      _ <- client.admin.getClusterHealth(Seq("twitter"), waitForStatus = Some(ClusterHealthStatus.YELLOW))
      _ <- client.admin.closeIndex("twitter")
      o <- client.admin.openIndex("twitter")
    } yield o

    whenReady(open) { v =>
      v should be('Acknowledged)
    }
  }

  it should "put mapping on an existing index" in new Setup {
    val mapping = Map("id" -> "type=long,store=true", "message" -> "type=string,store=true")
    val get = for {
      c <- client.admin.createIndex("twitter")
      p <- client.admin.putMapping("twitter", "tweet", mapping)
      g <- client.admin.getMapping("twitter", "tweet")
    } yield g

    whenReady(get) { v =>
      val mappings = v.mappings.get("twitter")
      mappings should not be null

      val tweetMapping = mappings.get("tweet")
      tweetMapping should not be null

      // Converting Java mapping source map to Scala map
      val tweetMappingMap = tweetMapping.sourceAsMap.toMap.get("properties") match {
        case Some(m) => m.asInstanceOf[util.Map[String, String]].toMap
        case None => Map()
      }

      tweetMappingMap should contain key "id"
      tweetMappingMap should contain key "message"
    }
  }

}

object Twitter {

  case class Tweet(id: Long, message: String)

}
