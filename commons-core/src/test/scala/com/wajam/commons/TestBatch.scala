package com.wajam.commons

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec

class TestBatch extends FlatSpec with ShouldMatchers {

  "batchCall function" should "never call batched function on empty sequence" in {
    def batch(i: Seq[Int]): String = fail("Should never be called")
    def fold(ls: List[String]): String = {
      ls should be('empty)
      ls.mkString("")
    }
    batchCall(Nil, 100)(batch)(fold)
  }

  it should "call once if batch size bigger than list" in {
    val list = Seq(1, 2, 3)

    def batch(seqI: Seq[Int]): Seq[String] = {
      seqI should be(list)
      seqI.map(_.toString)
    }
    def fold(ls: List[Seq[String]]): Seq[String] = {
      ls should not be ('empty)
      ls should have size (1)
      ls.flatten
    }
    val result = batchCall(list, 100)(batch)(fold)
    list.map(_.toString) should be(result)
  }

  it should "call more than once if batch size smaller than list" in {
    val list = Seq(1, 2, 3)

    def batch(seqI: Seq[Int]): Seq[String] = {
      seqI.foreach(i => list should contain(i))
      seqI.map(_.toString)
    }
    def fold(ls: List[Seq[String]]): Seq[String] = {
      ls should not be ('empty)
      ls should have size (2)
      ls.flatten
    }
    val result = batchCall(list, 2)(batch)(fold)
    list.map(_.toString) should be(result)
  }

  it should "use an optimal batch size" in {
    val list = Seq(1, 2, 3, 4, 5, 6)

    def batch(seqI: Seq[Int]): Seq[String] = {
      seqI should have size (3)
      seqI.foreach(i => list should contain(i))
      seqI.map(_.toString)
    }
    def fold(ls: List[Seq[String]]): Seq[String] = {
      ls should not be ('empty)
      ls should have size (2)
      ls.flatten
    }
    val result = batchCall(list, 4)(batch)(fold)
    list.map(_.toString) should be(result)
  }
}
