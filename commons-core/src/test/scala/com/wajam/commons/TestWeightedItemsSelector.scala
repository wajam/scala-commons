package com.wajam.commons

import scala.util.Random

import org.scalatest.{Matchers, FlatSpec}

class TestWeightedItemsSelector extends FlatSpec
with Matchers {

  implicit def random = new Random(13)

  "WeightedItemsSelector" should "select item according to weights" in {

    val fields = List((1.0, "a"), (3.0, "b"), (1.0, "c"))

    val selector = new WeightedItemsSelector[String](fields)

    val selected = for (i <- 1 to 100) yield selector.next

    val grouped = selected.groupBy(name => name)

    grouped("a").size should be(21)
    grouped("b").size should be(57)
    grouped("c").size should be(22)
  }

  it should "work with fractional weights" in {

    val fields = List((0.5, "a"), (1.5, "b"), (0.5, "c"))

    val selector = new WeightedItemsSelector[String](fields)

    val selected = for (i <- 1 to 100) yield selector.next

    val grouped = selected.groupBy(name => name)

    grouped("a").size should be(21)
    grouped("b").size should be(57)
    grouped("c").size should be(22)
  }


}
