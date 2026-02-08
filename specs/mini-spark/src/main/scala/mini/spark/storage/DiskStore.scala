package mini.spark.storage

import java.io.{EOFException, File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.{Files, StandardCopyOption}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DiskStore(val baseDir: File, val ttlMs: Long) extends Serializable {
  baseDir.mkdirs()
  private val lastAccess = mutable.HashMap.empty[BlockId, Long]

  def get(blockId: BlockId): Option[Any] = {
    evictExpired()
    val file = dataFile(blockId)
    if (!file.exists()) {
      lastAccess.remove(blockId)
      return None
    }

    if (isExpired(blockId)) {
      remove(blockId)
      return None
    }

    touch(blockId)

    val in = new ObjectInputStream(new FileInputStream(file))
    try {
      val first = in.readObject()
      first match {
        case seq: Seq[_] =>
          Some(seq)
        case other =>
          val buffer = new ArrayBuffer[Any]()
          buffer += other
          try {
            while (true) {
              buffer += in.readObject()
            }
          } catch {
            case _: EOFException => // end of stream
          }
          Some(buffer.toVector)
      }
    } finally {
      in.close()
    }
  }

  def put(blockId: BlockId, value: Any, attemptId: Int): Unit = {
    evictExpired()
    val temp = tempFile(blockId, attemptId)
    temp.getParentFile.mkdirs()
    val out = new ObjectOutputStream(new FileOutputStream(temp))
    try {
      out.writeObject(value)
    } finally {
      out.close()
    }
    commitTemp(temp, dataFile(blockId))
    touch(blockId)
  }

  def remove(blockId: BlockId): Unit = {
    val file = dataFile(blockId)
    if (file.exists()) {
      file.delete()
    }
    lastAccess.remove(blockId)
  }

  def clear(): Unit = {
    deleteRecursively(baseDir)
    baseDir.mkdirs()
    lastAccess.clear()
  }

  private[spark] def dataFile(blockId: BlockId): File =
    new File(baseDir, s"${blockId.name}.data")

  private[spark] def tempFile(blockId: BlockId, attemptId: Int): File =
    new File(baseDir, s"${blockId.name}.attempt_$attemptId.tmp")

  private[spark] def commitTemp(temp: File, dest: File): Unit = {
    try {
      Files.move(
        temp.toPath,
        dest.toPath,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE
      )
    } catch {
      case _: Exception =>
        Files.move(temp.toPath, dest.toPath, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  private[spark] def deleteTemp(temp: File): Unit = {
    if (temp.exists()) {
      temp.delete()
    }
  }

  private[spark] def touch(blockId: BlockId): Unit = {
    lastAccess.update(blockId, nowMs())
  }

  private def isExpired(blockId: BlockId): Boolean = {
    if (ttlMs <= 0) return false
    lastAccess.get(blockId).exists(ts => nowMs() - ts > ttlMs)
  }

  private def evictExpired(): Unit = {
    if (ttlMs <= 0) return
    val now = nowMs()
    val expired = lastAccess.collect { case (id, ts) if now - ts > ttlMs => id }
    expired.foreach(remove)
  }

  private def nowMs(): Long = System.currentTimeMillis()

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = Option(file.listFiles()).getOrElse(Array.empty)
      children.foreach(deleteRecursively)
    }
    file.delete()
  }
}
