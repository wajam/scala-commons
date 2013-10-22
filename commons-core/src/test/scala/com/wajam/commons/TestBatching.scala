package com.wajam.commons

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec
import com.wajam.commons.Batching._

class TestBatching extends FlatSpec with ShouldMatchers {

  "splitBatch function" should "return empty on empty sequence" in {
    splitBatch(Nil, 100).toList should be('empty)
  }

  it should "return only one batch if batch size bigger than list" in {
    val list = Seq(1, 2, 3)
    val result = splitBatch(list, 100).toList
    list should be(result.flatten)
    result should not be ('empty)
    result should have size (1)
  }

  it should "return more than one batch if size smaller than list" in {
    val list = Seq(1, 2, 3, 4)

    val result = splitBatch(list, 2).toList
    list should be(result.flatten)
    result should not be ('empty)
    result should have size (2)
    result should be(Seq(Seq(1, 2), Seq(3, 4)))
  }

  it should "allow for partial batches" in {
    val list = Seq(1, 2, 3, 4, 5, 6)

    val result = splitBatch(list, 4).toList
    list should be(result.flatten)
    result should not be ('empty)
    result should have size (2)
    result should be(Seq(Seq(1, 2, 3, 4), Seq(5, 6)))
  }

  "batchCall function" should "never call batched function on empty sequence" in {
    def batch(i: Seq[Int]): String = fail("Should never be called")

    batchCall(Nil, 100)(batch).toList should be('empty)
  }

  it should "call once if batch size bigger than list" in {
    val list = Seq(1, 2, 3)

    def batch(seqI: Seq[Int]): Seq[String] = {
      seqI should be(list)
      seqI.map(_.toString)
    }

    val result = batchCall(list, 100)(batch).toList
    list.map(_.toString) should be(result.flatten)
    result should not be ('empty)
    result should have size (1)
  }

  it should "call more than once if batch size smaller than list" in {
    val list = Seq(1, 2, 3, 4)

    def batch(seqI: Seq[Int]): Seq[String] = {
      seqI.foreach(i => list should contain(i))
      seqI.map(_.toString)
    }

    val result = batchCall(list, 2)(batch).toList
    list.map(_.toString) should be(result.flatten)
    result should not be ('empty)
    result should have size (2)
  }

}
