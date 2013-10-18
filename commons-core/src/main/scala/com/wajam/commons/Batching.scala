package com.wajam.commons

import scala.annotation.tailrec

/**
 * Collection of loose functions that can be used in any project
 */
object Batching {

  def splitBatch[I](toProcess: Seq[I], maxBatchSize: Int): List[Seq[I]] = {
    val batchQty = math.ceil(toProcess.size.toDouble / maxBatchSize)
    val actualBatchSize = math.min(math.ceil(toProcess.size.toDouble / batchQty), maxBatchSize).toInt

    @tailrec def processSplit(toSplit: Seq[I], splitted: List[Seq[I]]): List[Seq[I]] = {
      toSplit.splitAt(actualBatchSize) match {
        case (Seq(), Seq()) => splitted.reverse
        case (batch, next) => processSplit(next, batch :: splitted)
      }
    }

    processSplit(toProcess, Nil)
  }

  def batchCall[I, O](toProcess: Seq[I], maxBatchSize: Int)
                     (batchCall: Seq[I] => O): List[O] = {
    splitBatch(toProcess, maxBatchSize) map batchCall
  }

}
