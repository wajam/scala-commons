package com.wajam.commons

/**
 * Trait that renders an object observable by allowing event listening and triggering.
 */
trait Observable {
  private type Observer = (Event) => Unit
  private var observers = List[Observer]()
  private var parents = List[Observable]()

  def addParentObservable(parent: Observable) {
    assert(parent != this)
    this.parents :+= parent
  }

  def addObserver(cb: Observer) {
    this.observers :+= cb
  }

  def removeParentObservable(parent: Observable) {
    this.observers = this.observers diff List(parent)
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


