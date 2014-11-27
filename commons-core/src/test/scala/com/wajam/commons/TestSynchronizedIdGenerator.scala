package com.wajam.commons

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestSynchronizedIdGenerator extends FunSuite {

  class SequenceIdGenerator extends IdGenerator[Int] {
    var lastId = 0

    def nextId = {
      lastId += 1
      lastId
    }
  }

  ignore("concurent calls should get duplicates without synchronized") {
    val generator = new SequenceIdGenerator
    val iterations = 20000

    // Generate ids concurently
    val workers = 0.to(15).map(_ => Future({
      for (i <- 1 to iterations) yield generator.nextId
    }))

    val ids = Await.result(Future.sequence(workers), Duration.Inf).flatten
    ids.size should be(workers.size * iterations)
    ids.size should be > ids.distinct.size
  }

  test("concurent calls should not get any duplicates") {
    val generator = new SequenceIdGenerator with SynchronizedIdGenerator[Int]
    val iterations = 20000

    // Generate ids concurently
    val workers = 0.to(15).map(_ => Future({
      for (i <- 1 to iterations) yield generator.nextId
    }))

    val ids = Await.result(Future.sequence(workers), Duration.Inf).flatten
    ids.size should be(workers.size * iterations)
    ids.size should be(ids.distinct.size)
  }
}
