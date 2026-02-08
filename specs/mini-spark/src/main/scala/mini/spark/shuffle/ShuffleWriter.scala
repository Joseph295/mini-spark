package mini.spark.shuffle

trait ShuffleWriter[K, V] extends Serializable {
  def write(records: Iterator[(K, V)], attemptId: Int): Unit
}
