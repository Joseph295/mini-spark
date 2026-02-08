package mini.spark.storage

sealed trait BlockId extends Serializable {
  def name: String
}

case class RDDPartitionBlockId(rddId: Int, partitionId: Int) extends BlockId {
  override val name: String = s"rdd_${rddId}_$partitionId"
}

case class ShuffleBlockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override val name: String = s"shuffle_${shuffleId}_map_${mapId}_reduce_${reduceId}"
}
