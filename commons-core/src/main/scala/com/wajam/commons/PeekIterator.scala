package com.wajam.commons

/**
 * Iterator decorator which allows peeking at the next element. This implementation read ahead the next element.
 * <br/>
 * One should discard the decorated iterator, and use only the new PeekIterator. Using the old iterator is undefined,
 * subject to change, and may result in changes to the new iterator as well.
 */
class PeekIterator[T](itr: Iterator[T]) extends BufferedIterator[T] {
  self =>

  private var nextElem: Option[T] = getNextElem()

  def peek: T = nextElem.get

  def head: T = peek

  def hasNext = {
    nextElem.isDefined
  }

  def next() = {
    val value = nextElem
    nextElem = getNextElem()
    value.get
  }

  /**
   * Unlike takeWhile, this implementation leaves the original iterator safe to use,
   * although it will lack all the elements returned here – but nothing else
   */
  def listWhile(p: (T) => Boolean): List[T] = {
    new Iterator[T] {
      def hasNext = self.hasNext && p(self.peek)
      def next() = if (p(self.peek)) self.next() else None.get
    }.toList
  }

  /**
   * Partition this iterator into `Iterator[Seq[T]]` where each sequence match consecutively the same transformation
   * function value.
   */
  def sequenceBy[K](f: (T) => K): Iterator[Seq[T]] = {
    new Iterator[Seq[T]] {
      def hasNext = self.hasNext
      def next() = {
        val k = f(self.peek)
        listWhile(f(_) == k)
      }
    }
  }

  private def getNextElem(): Option[T] = {
    if (itr.hasNext) {
      Some(itr.next())
    } else None
  }
}

object PeekIterator {
  def apply[T](itr: Iterator[T]): PeekIterator[T] = new PeekIterator(itr)
}

object ClosablePeekIterator {
  def apply[T](itr: Iterator[T] with Closable): PeekIterator[T] with Closable = new PeekIterator(itr) with Closable {
    def close() = itr.close()
  }
}

/**
 * Ordering implementation to sort PeekIterator by peeked head value.
 */
class PeekIteratorOrdering[T](implicit ord: Ordering[T]) extends Ordering[PeekIterator[T]] {
  def compare(x: PeekIterator[T], y: PeekIterator[T]) = {
    (x.hasNext, y.hasNext) match {
      case (true, true) => ord.compare(x.peek, y.peek)
      case (true, false) => 1
      case (false, true) => -1
      case (false, false) => 0
    }
  }
}
