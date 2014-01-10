package com.wajam.commons

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec

class TestObservable extends FlatSpec with ShouldMatchers {

  trait Builder {

    class ObservableWithTrigger extends Observable {
      def trigger(e: Event) = notifyObservers(e)
    }

    var triggered = false

    val observer = { (e: Event) =>
      triggered = true
    }
  }

  "an observable" should "notify its observers" in new Builder {
    val observable = new ObservableWithTrigger

    observable.addObserver(observer)

    observable.trigger(new Event)

    triggered should be(true)
  }

  it should "NOT notify its removed observers" in new Builder {
    val observable = new ObservableWithTrigger

    observable.addObserver(observer)
    observable.removeObserver(observer)

    observable.trigger(new Event)

    triggered should be(false)
  }

  it should "notify its parents" in new Builder {
    val parent = new ObservableWithTrigger
    val child = new ObservableWithTrigger

    child.addParentObservable(parent)

    parent.addObserver(observer)

    child.trigger(new Event)

    triggered should be(true)
  }

  it should "NOT notify its removed parents" in new Builder {
    val parent = new ObservableWithTrigger
    val child = new ObservableWithTrigger

    child.addParentObservable(parent)
    child.removeParentObservable(parent)

    parent.addObserver(observer)

    child.trigger(new Event)

    triggered should be(false)
  }
}
