package mini.spark.rdd

import mini.spark.{Partition, SparkContext, TaskContext}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

abstract class RDD[T](val sc: SparkContext) extends Serializable {
  def partitions: Array[Partition]

  protected def compute(partition: Partition, context: TaskContext): Iterator[T]

  private[spark] def dependencies: Seq[RDD[_]] = Seq.empty

  final private[spark] def iterator(partition: Partition, context: TaskContext): Iterator[T] =
    compute(partition, context)

  def map[U](f: T => U): RDD[U] = new MapRDD[T, U](this, f)

  def filter(f: T => Boolean): RDD[T] = new FilterRDD[T](this, f)

  def flatMap[U](f: T => TraversableOnce[U]): RDD[U] = new FlatMapRDD[T, U](this, f)

  def collect()(implicit ct: ClassTag[T]): Array[T] = {
    val parts = sc.runJob(this, (it: Iterator[T]) => it.toArray)
    parts.flatten
  }

  def count(): Long = {
    val counts = sc.runJob(this, (it: Iterator[T]) => {
      var c = 0L
      while (it.hasNext) {
        it.next()
        c += 1
      }
      c
    })
    counts.sum
  }

  def take(n: Int)(implicit ct: ClassTag[T]): Array[T] = {
    if (n <= 0) {
      Array.empty[T]
    } else {
      val buf = new ArrayBuffer[T](n)
      val parts = partitions
      var i = 0
      while (i < parts.length && buf.length < n) {
        val part = parts(i)
        val context = TaskContext(part.index)
        val it = iterator(part, context)
        while (it.hasNext && buf.length < n) {
          buf += it.next()
        }
        i += 1
      }
      buf.toArray
    }
  }
}
