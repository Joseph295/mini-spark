package mini.spark.shuffle

import mini.spark.Partitioner

trait ShuffleManager extends Serializable {
  def registerShuffle(
      shuffleId: Int,
      numMaps: Int,
      numReduces: Int,
      partitioner: Partitioner
  ): ShuffleHandle

  def getWriter[K, V](handle: ShuffleHandle, mapId: Int): ShuffleWriter[K, V]

  def getReader[K, V](handle: ShuffleHandle, reduceId: Int): ShuffleReader[K, V]
}
