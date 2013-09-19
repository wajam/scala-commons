package com.wajam.caching

import scala.concurrent.Future

trait CachingService {
  def get(key: String): Option[Any]

  def delete(key: String)

  def set(key: String, value: Any): Future[Boolean]

  def set(key: String, value: Any, expiry: Int): Future[Boolean]

  def add(key: String, value: Any): Future[Boolean]

  def add(key: String, value: Any, expiry: Int): Future[Boolean]

  def increment(key: String): Long

  def increment(key: String, by: Int): Long

  def increment(key: String, by: Int, defaultValue: Long): Long

  def increment(key: String, by: Int, defaultValue: Long, expiry: Int): Long

  def decrement(key: String): Long

  def decrement(key: String, by: Int): Long

  def decrement(key: String, by: Int, defaultValue: Long): Long

  def decrement(key: String, by: Int, defaultValue: Long, expiry: Int): Long
}
