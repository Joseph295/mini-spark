package mini.spark

import java.util.concurrent.atomic.AtomicInteger
import mini.spark.rdd.RDD
import mini.spark.shuffle.ShuffleHandle

class ShuffleDependency[K, V](
    override val rdd: RDD[(K, V)],
    val partitioner: Partitioner
) extends Dependency[(K, V)] {
  val shuffleId: Int = ShuffleDependency.newShuffleId()
  @transient private var _shuffleHandle: ShuffleHandle = _

  def shuffleHandle: ShuffleHandle = _shuffleHandle

  def setShuffleHandle(handle: ShuffleHandle): Unit = {
    _shuffleHandle = handle
  }
}

object ShuffleDependency {
  private val nextId = new AtomicInteger(0)

  def newShuffleId(): Int = nextId.getAndIncrement()
}
