package com.wajam.commons

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import java.io.IOException
import org.scalatest.matchers.ShouldMatchers

@RunWith(classOf[JUnitRunner])
class TestClosable extends FunSuite with MockitoSugar with ShouldMatchers {
  test("should close after `using` complete normally") {
    var mockClosable = mock[Closable]

    import Closable.using
    using(mockClosable) { closable =>  }

    verify(mockClosable).close()
    verifyNoMoreInteractions(mockClosable)
  }

  test("should close after `using` exit with an exception") {
    var mockClosable = mock[Closable]

    evaluating {
      import Closable.using
      using(mockClosable) { closable => throw new IOException() }
    } should produce[IOException]

    verify(mockClosable).close()
    verifyNoMoreInteractions(mockClosable)
  }
}
