package mini.spark.shuffle

trait ShuffleReader[K, V] extends Serializable {
  def read(): Iterator[(K, V)]
}
