package mini.spark.shuffle

import java.io.{EOFException, File, FileInputStream, FileNotFoundException, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.{Files, StandardCopyOption}
import mini.spark.Partitioner
import scala.collection.mutable.ArrayBuffer

class SimpleShuffleManager(baseDir: File) extends ShuffleManager {
  baseDir.mkdirs()

  override def registerShuffle(
      shuffleId: Int,
      numMaps: Int,
      numReduces: Int,
      partitioner: Partitioner
  ): ShuffleHandle = {
    val dir = new File(baseDir, s"shuffle-$shuffleId")
    dir.mkdirs()
    ShuffleHandle(shuffleId, numMaps, numReduces, partitioner, dir)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int): ShuffleWriter[K, V] =
    new SimpleShuffleWriter[K, V](handle, mapId)

  override def getReader[K, V](handle: ShuffleHandle, reduceId: Int): ShuffleReader[K, V] =
    new SimpleShuffleReader[K, V](handle, reduceId)

  private[spark] def getShuffleDir(shuffleId: Int): File =
    new File(baseDir, s"shuffle-$shuffleId")

  private[spark] def cleanupAll(): Unit = {
    deleteRecursively(baseDir)
    baseDir.mkdirs()
  }

  private[spark] def getShuffleFile(handle: ShuffleHandle, mapId: Int, reduceId: Int): File = {
    new File(handle.shuffleDir, s"shuffle_${handle.shuffleId}_map_${mapId}_reduce_${reduceId}.data")
  }

  private[spark] def getTempShuffleFile(handle: ShuffleHandle, mapId: Int, reduceId: Int, attemptId: Int): File = {
    new File(
      handle.shuffleDir,
      s"shuffle_${handle.shuffleId}_map_${mapId}_reduce_${reduceId}_attempt_${attemptId}.tmp"
    )
  }

  private class SimpleShuffleWriter[K, V](handle: ShuffleHandle, mapId: Int) extends ShuffleWriter[K, V] {
    override def write(records: Iterator[(K, V)], attemptId: Int): Unit = {
      val tempFiles = Array.tabulate(handle.numReduces) { reduceId =>
        val file = getTempShuffleFile(handle, mapId, reduceId, attemptId)
        file.getParentFile.mkdirs()
        file
      }
      val streams = tempFiles.map(file => new ObjectOutputStream(new FileOutputStream(file)))
      try {
        records.foreach { case (k, v) =>
          val reduceId = handle.partitioner.getPartition(k)
          streams(reduceId).writeObject((k, v))
        }
      } finally {
        streams.foreach(_.close())
      }

      var reduceId = 0
      while (reduceId < handle.numReduces) {
        val temp = tempFiles(reduceId)
        val dest = getShuffleFile(handle, mapId, reduceId)
        if (dest.exists()) dest.delete()
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
        reduceId += 1
      }
    }
  }

  private class SimpleShuffleReader[K, V](handle: ShuffleHandle, reduceId: Int) extends ShuffleReader[K, V] {
    override def read(): Iterator[(K, V)] = {
      val buffer = new ArrayBuffer[(K, V)]()
      var mapId = 0
      while (mapId < handle.numMaps) {
        val file = getShuffleFile(handle, mapId, reduceId)
        if (!file.exists()) {
          throw new FileNotFoundException(
            s"Missing shuffle output: shuffle=${handle.shuffleId} map=$mapId reduce=$reduceId"
          )
        }
        val in = new ObjectInputStream(new FileInputStream(file))
        try {
          while (true) {
            buffer += in.readObject().asInstanceOf[(K, V)]
          }
        } catch {
          case _: EOFException => // end of stream
        } finally {
          in.close()
        }
        mapId += 1
      }
      buffer.iterator
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = Option(file.listFiles()).getOrElse(Array.empty)
      children.foreach(deleteRecursively)
    }
    file.delete()
  }
}
