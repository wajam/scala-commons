package com.wajam.caching

import java.net.InetSocketAddress
import com.wajam.commons.Logging
import net.spy.memcached.{DefaultConnectionFactory, MemcachedClient}
import scala.concurrent.{ExecutionContext, future}
import com.yammer.metrics.scala.Instrumented
import scala.collection.JavaConversions._
import java.util.concurrent.TimeUnit
import com.wajam.tracing.Traced

class SpyMemcachedCaching(configuration: SpyMemcachedConfiguration)(implicit ec: ExecutionContext)
  extends CachingService with Logging with Instrumented with Traced {

  private val addrs = configuration.servers.map {
    address => new InetSocketAddress(address, configuration.port)
  }

  private val conFactory = new DefaultConnectionFactory {
    override def getOperationTimeout = configuration.timeout
  }

  private val numClients = configuration.clientNb
  private val clients = (1 to numClients) map {
    _ => new MemcachedClient(conFactory, addrs)
  }

  private val metricGet = tracedTimer("memcached-get")
  private val metricSet = tracedTimer("memcached-set")
  private val metricDelete = tracedTimer("memcached-delete")
  private val metricAdd = tracedTimer("memcached-add")
  private val metricIncrement = tracedTimer("memcached-increment")
  private val metricDecrement = tracedTimer("memcached-decrement")

  private val metricCacheHit = metrics.meter("memcached-hit", "hits")
  private val metricCacheMiss = metrics.meter("memcached-miss", "misses")

  def get(key: String) = {

    val client = getClient

    val cachedValue = try {
      metricGet.time {
        Option(client.get(key))
      }
    } catch {
      case e: Exception => {
        error("Couln't get object {} from memcache server {} : {}", key, getAddressFor(client, key), e)
        None
      }
    }

    if (cachedValue.isDefined) {
      metricCacheHit.mark()
    } else {
      metricCacheMiss.mark()
    }

    cachedValue
  }

  def delete(key: String) {
    val client = getClient
    metricDelete.time {
      client.delete(key)
    }
  }

  def set(key: String, value: Any) = {
    set(key, value, 0)
  }

  def set(key: String, value: Any, expiry: Int) = future[Boolean] {
    val client = getClient
    try {
      metricSet.time {
        client.set(key, expiry, value).get(configuration.timeout, TimeUnit.MILLISECONDS)
      }
    } catch {
      case e: Exception => {
        error("Couln't set object {} into memcache server {} : {}", key, getAddressFor(client, key), e)
        false
      }
    }
  }

  def add(key: String, value: Any) = {
    add(key, value, 0)
  }

  def add(key: String, value: Any, expiry: Int) = future[Boolean] {
    val client = getClient
    try {
      metricAdd.time {
        client.add(key, expiry, value).get(configuration.timeout, TimeUnit.MILLISECONDS)
      }
    } catch {
      case e: Exception => {
        error("Couln't add object {} into memcache server {} : {}", key, getAddressFor(client, key), e)
        false
      }
    }
  }

  def increment(key: String) = {
    increment(key, 1)
  }

  def increment(key: String, by: Int) = {
    metricIncrement.time {
      val client = getClient
      client.incr(key, by)
    }
  }

  def increment(key: String, by: Int, defaultValue: Long) = {
    metricIncrement.time {
      val client = getClient
      client.incr(key, by, defaultValue)
    }
  }

  def increment(key: String, by: Int, defaultValue: Long, expiry: Int) = {
    metricIncrement.time {
      val client = getClient
      client.incr(key, by, defaultValue, expiry)
    }
  }

  def decrement(key: String) = {
    decrement(key, 1)
  }

  def decrement(key: String, by: Int) = {
    metricDecrement.time {
      val client = getClient
      client.decr(key, by)
    }
  }

  def decrement(key: String, by: Int, defaultValue: Long) = {
    metricDecrement.time {
      val client = getClient
      client.decr(key, by, defaultValue)
    }
  }

  def decrement(key: String, by: Int, defaultValue: Long, expiry: Int) = {
    metricDecrement.time {
      val client = getClient
      client.decr(key, by, defaultValue, expiry)
    }
  }

  private def getClient = {
    clients(new scala.util.Random().nextInt(numClients))
  }

  private def getAddressFor(client: MemcachedClient, key: String) = {
    client.getNodeLocator.getPrimary(key).getSocketAddress.asInstanceOf[InetSocketAddress].getHostString
  }

}

case class SpyMemcachedConfiguration(servers: Seq[String], port: Int,
                                     timeout: Int, clientNb: Int)
