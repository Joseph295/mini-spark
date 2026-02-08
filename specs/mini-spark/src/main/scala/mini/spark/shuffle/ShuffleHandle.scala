package mini.spark.shuffle

import java.io.File
import mini.spark.Partitioner

case class ShuffleHandle(
    shuffleId: Int,
    numMaps: Int,
    numReduces: Int,
    partitioner: Partitioner,
    shuffleDir: File
) extends Serializable
