package mini.spark

import mini.spark.rdd.{ParallelCollectionRDD, RDD}
import mini.spark.scheduler.{DAGScheduler, TaskScheduler, TaskSchedulerImpl}
import mini.spark.shuffle.{ShuffleManager, SimpleShuffleManager}
import scala.reflect.ClassTag

class SparkContext {
  private[spark] val shuffleManager: ShuffleManager = new SimpleShuffleManager
  private[spark] val taskScheduler: TaskScheduler = new TaskSchedulerImpl
  private[spark] val dagScheduler: DAGScheduler = new DAGScheduler(this)

  def parallelize[T](seq: Seq[T], numSlices: Int = SparkContext.DefaultParallelism): RDD[T] = {
    val slices = if (numSlices <= 0) 1 else numSlices
    new ParallelCollectionRDD[T](this, seq, slices)
  }

  def runJob[T, U: ClassTag](rdd: RDD[T], func: Iterator[T] => U): Array[U] = {
    dagScheduler.runJob(rdd, func)
  }
}

object SparkContext {
  val DefaultParallelism: Int = math.max(1, Runtime.getRuntime.availableProcessors())
}
