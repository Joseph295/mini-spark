package mini.spark

trait Partitioner extends Serializable {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

class HashPartitioner(val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = {
    val raw = if (key == null) 0 else key.hashCode()
    (raw & Int.MaxValue) % numPartitions
  }
}
