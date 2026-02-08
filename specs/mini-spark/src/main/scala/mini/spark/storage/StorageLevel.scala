package mini.spark.storage

case class StorageLevel(useMemory: Boolean, useDisk: Boolean) extends Serializable

object StorageLevel {
  val NONE: StorageLevel = StorageLevel(useMemory = false, useDisk = false)
  val MEMORY_ONLY: StorageLevel = StorageLevel(useMemory = true, useDisk = false)
  val MEMORY_AND_DISK: StorageLevel = StorageLevel(useMemory = true, useDisk = true)
}
