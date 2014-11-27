package com.wajam.commons

import java.io.IOException

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FunSuite, Matchers }

@RunWith(classOf[JUnitRunner])
class TestClosable extends FunSuite with MockitoSugar with Matchers {
  test("should close after `using` complete normally") {
    val mockClosable = mock[Closable]

    import com.wajam.commons.Closable.using
    using(mockClosable) { closable => }

    verify(mockClosable).close()
  }

  test("should close after `using` exit with an exception") {
    val mockClosable = mock[Closable]

    evaluating {
      import com.wajam.commons.Closable.using
      using(mockClosable) { closable => throw new IOException() }
    } should produce[IOException]

    verify(mockClosable).close()
  }

  test("should close non Closable with close") {

    trait NotClosableWithClose {
      def close()
    }

    val mockNotClosableWithClose = mock[NotClosableWithClose]

    import com.wajam.commons.Closable._
    using(mockNotClosableWithClose) { closable => }

    verify(mockNotClosableWithClose).close()
  }
}
