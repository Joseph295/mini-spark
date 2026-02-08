package mini.spark

import mini.spark.rdd.RDD

class NarrowDependency[T](override val rdd: RDD[T]) extends Dependency[T]
