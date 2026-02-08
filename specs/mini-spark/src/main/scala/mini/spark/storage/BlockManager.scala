package mini.spark.storage

import java.io.File
import mini.spark.TaskContext
import scala.collection.mutable

class BlockManager(val memoryStore: MemoryStore, val diskStore: DiskStore) extends Serializable {
  private case class TaskAttemptKey(stageId: Int, partitionId: Int, attemptId: Int)
  private case class PendingBlock(
      blockId: BlockId,
      value: Option[Seq[Any]],
      storageLevel: StorageLevel,
      diskTemp: Option[File]
  )

  private val pending = mutable.HashMap.empty[TaskAttemptKey, mutable.ArrayBuffer[PendingBlock]]

  def get(blockId: BlockId): Option[Any] = {
    memoryStore.get(blockId).orElse(diskStore.get(blockId))
  }

  def put(blockId: BlockId, value: Any, storageLevel: StorageLevel): Unit = {
    put(blockId, value, storageLevel, attemptId = 0)
  }

  def put(blockId: BlockId, value: Any, storageLevel: StorageLevel, attemptId: Int): Unit = {
    if (!storageLevel.useMemory && !storageLevel.useDisk) return

    if (storageLevel.useMemory) {
      val stored = memoryStore.put(blockId, value)
      if (stored) return
    }

    if (storageLevel.useDisk) {
      diskStore.put(blockId, value, attemptId)
    }
  }

  private[spark] def addPending(
      blockId: BlockId,
      value: Option[Seq[Any]],
      storageLevel: StorageLevel,
      diskTemp: Option[File],
      context: TaskContext
  ): Unit = {
    if (!storageLevel.useMemory && !storageLevel.useDisk) return
    val key = TaskAttemptKey(context.stageId, context.partitionId, context.attemptId)
    val list = pending.getOrElseUpdate(key, mutable.ArrayBuffer.empty)
    list += PendingBlock(blockId, value, storageLevel, diskTemp)
  }

  private[spark] def commitTask(context: TaskContext): Unit = {
    val key = TaskAttemptKey(context.stageId, context.partitionId, context.attemptId)
    pending.remove(key).foreach { blocks =>
      blocks.foreach { block =>
        var storedInMemory = false
        if (block.storageLevel.useMemory) {
          block.value.foreach { v =>
            storedInMemory = memoryStore.put(block.blockId, v)
          }
        }

        if (block.storageLevel.useDisk && !storedInMemory) {
          block.diskTemp match {
            case Some(temp) =>
              diskStore.commitTemp(temp, diskStore.dataFile(block.blockId))
              diskStore.touch(block.blockId)
            case None =>
              block.value.foreach { v =>
                diskStore.put(block.blockId, v, context.attemptId)
              }
          }
        } else {
          block.diskTemp.foreach(diskStore.deleteTemp)
        }
      }
    }
  }

  private[spark] def abortTask(context: TaskContext): Unit = {
    val key = TaskAttemptKey(context.stageId, context.partitionId, context.attemptId)
    pending.remove(key).foreach { blocks =>
      blocks.foreach { block =>
        block.diskTemp.foreach(diskStore.deleteTemp)
      }
    }
  }

  def remove(blockId: BlockId): Unit = {
    memoryStore.remove(blockId)
    diskStore.remove(blockId)
  }

  private[spark] def clearAll(): Unit = {
    memoryStore.clear()
    diskStore.clear()
  }
}
