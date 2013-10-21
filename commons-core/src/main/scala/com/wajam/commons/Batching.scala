package com.wajam.commons

/**
 * Collection of loose functions that can be used in any project
 */
object Batching {

  def splitBatch[I](toProcess: Seq[I], maxBatchSize: Int): Iterator[Seq[I]] = {
    val batchQty = math.ceil(toProcess.size.toDouble / maxBatchSize)
    val actualBatchSize = math.min(math.ceil(toProcess.size.toDouble / batchQty), maxBatchSize).toInt

    if (actualBatchSize == 0) Iterator.empty
    else toProcess.grouped(actualBatchSize)
  }

  def batchCall[I, O](toProcess: Seq[I], maxBatchSize: Int)
                     (batchCall: Seq[I] => O): Iterator[O] = {
    splitBatch(toProcess, maxBatchSize) map batchCall
  }

}
