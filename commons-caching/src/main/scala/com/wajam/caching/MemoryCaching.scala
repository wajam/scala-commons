package com.wajam.caching

import scala.concurrent.Future

/**
 * In-memory caching that emulate memcached.
 *
 * Doesn't support expiry at the moment.
 * Not threadsafe.
 */
class MemoryCaching extends CachingService {

  private val localCache = new collection.mutable.HashMap[String, Any]

  private var getHits = 0
  private var get = 0

  private var set = 0

  private var deleteHits = 0
  private var delete = 0

  private var incrementHits = 0
  private var decrementHits = 0

  def get(key: String): Option[Any] = {
    val hit = localCache.get(key)

    get = get + 1

    if (hit.isDefined)
      getHits = getHits + 1

    hit
  }

  def delete(key: String) {
    val hit = localCache.remove(key)

    delete = delete + 1

    if (hit.isDefined)
      deleteHits = deleteHits + 1
  }

  def set(key: String, value: Any): Future[Boolean] = {

    localCache += (key -> value)

    set = set + 1

    Future.successful(true)
  }

  def set(key: String, value: Any, expiry: Int): Future[Boolean] = {
    set(key, value)
  }

  def add(key: String, value: Any): Future[Boolean] = {
    get(key) match {
      case None => set(key, value)
      case _ => Future.successful(false)
    }
  }

  def add(key: String, value: Any, expiry: Int): Future[Boolean] = {
    add(key, value)
  }

  def increment(key: String): Long = {
    increment(key, 1)
  }

  def increment(key: String, by: Int): Long = {
    increment(key, by, 0)
  }

  def increment(key: String, by: Int, defaultValue: Long): Long = {
    val hit = localCache.get(key)

    incrementHits = incrementHits + 1

    val value = hit match {
      case None => defaultValue
      case Some(v) => v.asInstanceOf[Long] + by
    }

    localCache += (key -> value)

    value
  }

  def increment(key: String, by: Int, defaultValue: Long, expiry: Int): Long = {
    increment(key, by, defaultValue)
  }

  def decrement(key: String): Long = {
    decrement(key, 1)
  }

  def decrement(key: String, by: Int): Long = {
    decrement(key, by, 0)
  }

  def decrement(key: String, by: Int, defaultValue: Long): Long = {
    val hit = localCache.get(key)

    decrementHits = decrementHits + 1

    val value = hit match {
      case None => defaultValue
      case Some(v) => v.asInstanceOf[Long] - by
    }

    localCache += (key -> value)

    value
  }

  def decrement(key: String, by: Int, defaultValue: Long, expiry: Int): Long = {
    decrement(key, by, defaultValue)
  }

  def getHitsCount(): Int = getHits

  def getCount(): Int = get

  def deleteCount(): Int = delete

  def deleteHitsCount(): Int = deleteHits

  def setCount(): Int = set

  def incrementHitsCount(): Int = incrementHits

  def decrementHitsCount(): Int = decrementHits
}
