package com.wajam.commons

/**
 * Collection of loose functions that can be used in any project
 */
object Batching {

  def splitBatch[I](toProcess: Seq[I], batchSize: Int): Iterator[Seq[I]] = {
    if (toProcess.isEmpty) Iterator.empty
    else toProcess.grouped(batchSize)
  }

  def batchCall[I, O](toProcess: Seq[I], batchSize: Int)
                     (batchCall: Seq[I] => O): Iterator[O] = {
    splitBatch(toProcess, batchSize) map batchCall
  }

}
