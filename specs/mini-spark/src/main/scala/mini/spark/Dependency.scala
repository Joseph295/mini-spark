package mini.spark

import mini.spark.rdd.RDD

trait Dependency[T] extends Serializable {
  def rdd: RDD[T]
}
