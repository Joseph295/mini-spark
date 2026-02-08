package mini.spark

import java.io.File
import java.util.UUID
import mini.spark.rdd.{ParallelCollectionRDD, RDD}
import mini.spark.scheduler.{DAGScheduler, TaskScheduler, TaskSchedulerImpl}
import mini.spark.shuffle.{ShuffleManager, SimpleShuffleManager}
import mini.spark.storage.{BlockManager, DiskStore, MemoryStore}
import scala.reflect.ClassTag

class SparkContext(
    val maxTaskAttempts: Int = SparkContext.DefaultMaxTaskAttempts,
    val maxMemoryBytes: Long = SparkContext.DefaultMaxMemoryBytes,
    val blockTtlMs: Long = SparkContext.DefaultBlockTtlMs
) {
  private val appId = UUID.randomUUID().toString
  private[spark] val shuffleManager: ShuffleManager = new SimpleShuffleManager(new File(s"target/shuffle/$appId"))
  private[spark] val blockManager: BlockManager =
    new BlockManager(
      new MemoryStore(maxMemoryBytes, blockTtlMs),
      new DiskStore(new File(s"target/block-manager/$appId"), blockTtlMs)
    )
  private[spark] val taskScheduler: TaskScheduler = new TaskSchedulerImpl(maxTaskAttempts, blockManager)
  private[spark] val dagScheduler: DAGScheduler = new DAGScheduler(this)

  def parallelize[T](seq: Seq[T], numSlices: Int = SparkContext.DefaultParallelism): RDD[T] = {
    val slices = if (numSlices <= 0) 1 else numSlices
    new ParallelCollectionRDD[T](this, seq, slices)
  }

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    dagScheduler.runJob(rdd, func)
  }

  def stop(): Unit = {
    blockManager.clearAll()
    cleanupShuffle()
  }

  private[spark] def cleanupShuffle(): Unit = {
    shuffleManager match {
      case manager: SimpleShuffleManager => manager.cleanupAll()
      case _ =>
    }
  }
}

object SparkContext {
  val DefaultParallelism: Int = math.max(1, Runtime.getRuntime.availableProcessors())
  val DefaultMaxTaskAttempts: Int = 3
  val DefaultMaxMemoryBytes: Long = 64L * 1024 * 1024
  val DefaultBlockTtlMs: Long = 10L * 60 * 1000
}
