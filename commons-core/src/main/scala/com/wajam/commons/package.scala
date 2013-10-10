package com.wajam

import scala.annotation.tailrec

/**
 * Collection of loose functions that can be used in any project
 */
package object commons {

  def batchCall[I, O](toProcess: Seq[I], maxBatchSize: Int)
                     (batchCall: Seq[I] => O)
                     (fold: List[O] => O): O = {
    val batchQty = math.ceil(toProcess.size.toDouble / maxBatchSize)
    val actualBatchSize = math.min(math.ceil(toProcess.size.toDouble / batchQty), maxBatchSize).toInt

    @tailrec
    def processBatch(toProcess: Seq[I], processed: List[O] = Nil): O = {
      toProcess.splitAt(actualBatchSize) match {
        case (Nil, Nil) => fold(processed.reverse)
        case (batch, next) => processBatch(next, batchCall(batch) :: processed)
      }
    }

    processBatch(toProcess)
  }

}
