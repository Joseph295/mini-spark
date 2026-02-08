package mini.spark.storage

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import scala.collection.mutable

class MemoryStore(val maxMemoryBytes: Long, val ttlMs: Long) extends Serializable {
  private case class Entry(value: Any, size: Long, var lastAccessMs: Long)
  private val entries = mutable.LinkedHashMap.empty[BlockId, Entry]
  private var usedMemory: Long = 0L

  def get(blockId: BlockId): Option[Any] = {
    evictExpired()
    entries.get(blockId) match {
      case Some(entry) if isExpired(entry) =>
        remove(blockId)
        None
      case Some(entry) =>
        entry.lastAccessMs = nowMs()
        entries.remove(blockId)
        entries.put(blockId, entry)
        Some(entry.value)
      case None =>
        None
    }
  }

  def put(blockId: BlockId, value: Any): Boolean = {
    evictExpired()
    val size = estimateSize(value)
    if (size > maxMemoryBytes) return false

    entries.get(blockId).foreach { entry =>
      usedMemory -= entry.size
      entries.remove(blockId)
    }

    while (usedMemory + size > maxMemoryBytes && entries.nonEmpty) {
      val (oldestId, oldestEntry) = entries.head
      entries.remove(oldestId)
      usedMemory -= oldestEntry.size
    }

    if (usedMemory + size > maxMemoryBytes) return false

    val entry = Entry(value, size, nowMs())
    entries.put(blockId, entry)
    usedMemory += size
    true
  }

  def remove(blockId: BlockId): Unit = {
    entries.remove(blockId).foreach { entry => usedMemory -= entry.size }
  }

  def clear(): Unit = {
    entries.clear()
    usedMemory = 0L
  }

  private[spark] def estimateSize(value: Any): Long = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    try {
      out.writeObject(value)
      out.flush()
      bos.toByteArray.length.toLong
    } finally {
      out.close()
    }
  }

  private def evictExpired(): Unit = {
    if (ttlMs <= 0) return
    val now = nowMs()
    val expired = entries.collect { case (id, entry) if now - entry.lastAccessMs > ttlMs => id }
    expired.foreach(remove)
  }

  private def isExpired(entry: Entry): Boolean = ttlMs > 0 && (nowMs() - entry.lastAccessMs > ttlMs)

  private def nowMs(): Long = System.currentTimeMillis()
}
