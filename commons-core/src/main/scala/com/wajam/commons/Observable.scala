package com.wajam.commons

/**
 * Trait that renders an object observable by allowing event listening and triggering.
 */
trait Observable {
  private type Observer = (Event) => Unit
  private var observers = Set[Observer]()
  private var parents = Set[Observable]()

  def addObserver(cb: Observer) {
    observers.synchronized {
      observers += cb
    }
  }

  def removeObserver(cb: Observer) {
    observers.synchronized {
      observers -= cb
    }
  }

  def addParentObservable(parent: Observable) {
    assert(parent != this)
    parents.synchronized {
      parents += parent
    }
  }

  def removeParentObservable(parent: Observable) {
    parents.synchronized {
      parents -= parent
    }
  }

  protected def notifyObservers(event: Event) {
    observers.foreach(obs => obs(event))
    parents.foreach(parent => parent.notifyObservers(event))
  }
}

class Event

class VotableEvent extends Event {
  var yeaVotes = 0
  var nayVotes = 0

  def vote(pass: Boolean) = pass match {
    case true => yeaVotes += 1
    case false => nayVotes += 1
  }

}



