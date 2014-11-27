package com.wajam.commons

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

class TestRatioUtils extends FunSuite with MockitoSugar with Matchers {

  val ids = (0L until Math.pow(10, 4).toLong).toSeq

  test("ratio 0.0 should include nothing") {
    ids count { case id => RatioUtils.valueIncluded(id, 0.0) } should be(0)
  }

  test("ratio 1.0 should include all") {
    ids count { case id => RatioUtils.valueIncluded(id, 1.0) } should be(10000)
  }

  test("commons ratio should be consistent") {
    ids count { case id => RatioUtils.valueIncluded(id, 0.01) } should be(100)
    ids count { case id => RatioUtils.valueIncluded(id, 0.25) } should be(2500)
    ids count { case id => RatioUtils.valueIncluded(id, 0.50) } should be(5000)
    ids count { case id => RatioUtils.valueIncluded(id, 0.33) } should be(3300)
    ids count { case id => RatioUtils.valueIncluded(id, 0.66) } should be(6600)
    ids count { case id => RatioUtils.valueIncluded(id, 0.75) } should be(7500)
    ids count { case id => RatioUtils.valueIncluded(id, 0.99) } should be(9900)
  }
}
