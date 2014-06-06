package com.wajam.commons

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import java.util.NoSuchElementException
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import com.wajam.commons.Closable

@RunWith(classOf[JUnitRunner])
class TestPeekIterator extends FlatSpec with MockitoSugar {
  "Empty iterator" should "behave properly" in {
    val itr = PeekIterator(Iterator[String]())
    itr.hasNext should be(false)

    evaluating {
      itr.next()
    } should produce[NoSuchElementException]

    evaluating {
      itr.peek
    } should produce[NoSuchElementException]
  }

  "Non-empty iterator" should "behave properly with one item" in {
    val itr = PeekIterator(Iterator(1))
    itr.peek should be(1)
    itr.hasNext should be(true)
    itr.peek should be(1)
    itr.next() should be(1)

    itr.hasNext should be(false)

    evaluating {
      itr.next()
    } should produce[NoSuchElementException]

    evaluating {
      itr.peek
    } should produce[NoSuchElementException]
  }

  it should "behave properly with multiples items" in {
    val itr = PeekIterator(Iterator(1, 2, 3))
    itr.hasNext should be(true)
    itr.hasNext should be(true) // Call twice to ensure hasNext does not advance the cursor

    itr.peek should be(1)
    itr.next() should be(1)
    itr.hasNext should be(true)

    itr.next() should be(2)
    itr.hasNext should be(true)

    itr.peek should be(3)
    itr.next() should be(3)
    itr.hasNext should be(false)
  }

  "Closable iterator" should "be closed" in {
    trait ClosableIterator[T] extends Iterator[Int] with Closable

    val mockItr: ClosableIterator[Int] = mock[ClosableIterator[Int]]
    val itr = ClosablePeekIterator(mockItr)
    itr.close()

    verify(mockItr).close()
  }

  "Ordering" should "order iterators per head value" in {
    val itr0 = PeekIterator(Iterator[Int]())
    val itr1 = PeekIterator(Iterator(1, 2, 3))
    val itr2 = PeekIterator(Iterator(2, 3))
    val itr3 = PeekIterator(Iterator(3))

    implicit val ordering = new PeekIteratorOrdering[Int]

    val itrs = List(itr2, itr3, itr1, itr0)
    val sortedItrs = itrs.sorted

    sortedItrs should be(List(itr0, itr1, itr2, itr3))
  }
}
