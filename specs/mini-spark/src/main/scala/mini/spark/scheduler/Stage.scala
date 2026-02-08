package mini.spark.scheduler

import mini.spark.ShuffleDependency
import mini.spark.rdd.RDD

case class Stage(
    id: Int,
    rdd: RDD[_],
    parents: Seq[Stage],
    isShuffleMap: Boolean,
    shuffleDep: Option[ShuffleDependency[_, _]]
) extends Serializable
