package mini.spark.rdd

import java.io.{File, FileOutputStream, ObjectOutputStream}
import java.util.concurrent.atomic.AtomicInteger
import mini.spark.{Dependency, Partition, SparkContext, TaskContext}
import mini.spark.storage.{RDDPartitionBlockId, StorageLevel}
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

abstract class RDD[T](val sc: SparkContext) extends Serializable {
  val id: Int = RDD.newRddId()
  def partitions: Array[Partition]

  protected def compute(partition: Partition, context: TaskContext): Iterator[T]

  def dependencies: Seq[Dependency[_]] = Seq.empty

  final private[spark] def iterator(partition: Partition, context: TaskContext): Iterator[T] =
    storageLevel match {
      case level if level != StorageLevel.NONE =>
        val blockId = RDDPartitionBlockId(id, partition.index)
        sc.blockManager.get(blockId) match {
          case Some(cached: Seq[T] @unchecked) => cached.iterator
          case _ =>
            new CachedIterator(compute(partition, context), blockId, level, context)
        }
      case _ =>
        compute(partition, context)
    }

  private class CachedIterator(
      iter: Iterator[T],
      blockId: RDDPartitionBlockId,
      level: StorageLevel,
      context: TaskContext
  ) extends Iterator[T] {
    private val buffer = ArrayBuffer.empty[T]
    private val memoryStore = sc.blockManager.memoryStore
    private val diskStore = sc.blockManager.diskStore
    private val useMemory = level.useMemory
    private val useDisk = level.useDisk
    private var estimatedSize: Long = 0L
    private var spilled = false
    private var disabled = false
    private var finished = false
    private var tempFile: Option[File] = None
    private var diskOut: Option[ObjectOutputStream] = None

    override def hasNext: Boolean = {
      val hn = iter.hasNext
      if (!hn && !finished) {
        finish()
      }
      hn
    }

    override def next(): T = {
      try {
        val value = iter.next()
        if (!disabled) {
          if (useMemory && !spilled) {
            buffer += value
            estimatedSize += memoryStore.estimateSize(value)
            if (estimatedSize > memoryStore.maxMemoryBytes) {
              if (useDisk) {
                spillToDisk()
              } else {
                buffer.clear()
                disabled = true
              }
            }
          } else if (useDisk) {
            writeToDisk(value)
          }
        }
        value
      } catch {
        case t: Throwable =>
          abort()
          throw t
      }
    }

    private def spillToDisk(): Unit = {
      openDiskIfNeeded()
      buffer.foreach(writeToDisk)
      buffer.clear()
      spilled = true
    }

    private def openDiskIfNeeded(): Unit = {
      if (diskOut.isEmpty) {
        val temp = diskStore.tempFile(blockId, context.attemptId)
        temp.getParentFile.mkdirs()
        tempFile = Some(temp)
        diskOut = Some(new ObjectOutputStream(new FileOutputStream(temp)))
      }
    }

    private def writeToDisk(value: T): Unit = {
      openDiskIfNeeded()
      diskOut.foreach(_.writeObject(value))
    }

    private def finish(): Unit = {
      finished = true
      diskOut.foreach(_.close())

      if (!disabled) {
        val valueOpt: Option[Seq[Any]] =
          if (useMemory && !spilled) Some(buffer.toVector.asInstanceOf[Seq[Any]]) else None
        val diskOpt: Option[File] =
          if (useDisk && spilled) tempFile else None
        if (valueOpt.nonEmpty || diskOpt.nonEmpty) {
          sc.blockManager.addPending(blockId, valueOpt, level, diskOpt, context)
        }
      } else {
        tempFile.foreach(diskStore.deleteTemp)
      }
    }

    private def abort(): Unit = {
      diskOut.foreach(_.close())
      tempFile.foreach(diskStore.deleteTemp)
    }
  }

  private var storageLevel: StorageLevel = StorageLevel.NONE

  def persist(level: StorageLevel): this.type = {
    storageLevel = level
    this
  }

  def unpersist(): this.type = {
    if (storageLevel != StorageLevel.NONE) {
      partitions.foreach { p =>
        sc.blockManager.remove(RDDPartitionBlockId(id, p.index))
      }
    }
    storageLevel = StorageLevel.NONE
    this
  }

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
    if (n <= 0) Array.empty[T] else collect().take(n)
  }
}

object RDD {
  private val nextRddId = new AtomicInteger(0)

  private[rdd] def newRddId(): Int = nextRddId.getAndIncrement()

  implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)]): PairRDDFunctions[K, V] =
    new PairRDDFunctions(rdd)
}
