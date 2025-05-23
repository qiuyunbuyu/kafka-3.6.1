/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.io.{BufferedReader, BufferedWriter, File, FileOutputStream, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern

import kafka.utils.Logging
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.{InconsistentTopicIdException, KafkaStorageException}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.storage.internals.log.LogDirFailureChannel


object PartitionMetadataFile {
  private val PartitionMetadataFilename = "partition.metadata"
  private val WhiteSpacesPattern = Pattern.compile(":\\s+")
  private val CurrentVersion = 0

  def newFile(dir: File): File = new File(dir, PartitionMetadataFilename)

  object PartitionMetadataFileFormatter {
    def toFile(data: PartitionMetadata): String = {
      s"version: ${data.version}\ntopic_id: ${data.topicId}"
    }

  }

  class PartitionMetadataReadBuffer[T](location: String,
                                       reader: BufferedReader) extends Logging {
    def read(): PartitionMetadata = {
      def malformedLineException(line: String) =
        new IOException(s"Malformed line in checkpoint file ($location): '$line'")

      var line: String = null
      var metadataTopicId: Uuid = null
      try {
        line = reader.readLine()
        WhiteSpacesPattern.split(line) match {
          case Array(_, version) =>
            if (version.toInt == CurrentVersion) {
              line = reader.readLine()
              WhiteSpacesPattern.split(line) match {
                case Array(_, topicId) => metadataTopicId = Uuid.fromString(topicId)
                case _ => throw malformedLineException(line)
              }
              if (metadataTopicId.equals(Uuid.ZERO_UUID)) {
                throw new IOException(s"Invalid topic ID in partition metadata file ($location)")
              }
              new PartitionMetadata(CurrentVersion, metadataTopicId)
            } else {
              throw new IOException(s"Unrecognized version of partition metadata file ($location): " + version)
            }
          case _ => throw malformedLineException(line)
        }
      } catch {
        case _: NumberFormatException => throw malformedLineException(line)
      }
    }
  }

}

class PartitionMetadata(val version: Int, val topicId: Uuid)


class PartitionMetadataFile(val file: File,
                            logDirFailureChannel: LogDirFailureChannel) extends Logging {
  import kafka.server.PartitionMetadataFile.{CurrentVersion, PartitionMetadataFileFormatter, PartitionMetadataReadBuffer}

  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  private val logDir = file.getParentFile.getParent
  @volatile private var dirtyTopicIdOpt : Option[Uuid] = None

  /**
   * Records the topic ID that will be flushed to disk.
   */
  def record(topicId: Uuid): Unit = {
    // Topic IDs should not differ, but we defensively check here to fail earlier in the case that the IDs somehow differ.
    dirtyTopicIdOpt.foreach { dirtyTopicId =>
      if (dirtyTopicId != topicId)
        throw new InconsistentTopicIdException(s"Tried to record topic ID $topicId to file " +
          s"but had already recorded $dirtyTopicId")
    }
    dirtyTopicIdOpt = Some(topicId)
  }

  def maybeFlush(): Unit = {
    // We check dirtyTopicId first to avoid having to take the lock unnecessarily in the frequently called log append path
    dirtyTopicIdOpt.foreach { _ =>
      // We synchronize on the actual write to disk
      lock synchronized {
        dirtyTopicIdOpt.foreach { topicId =>
          try {
            // write to temp file and then swap with the existing file
            val fileOutputStream = new FileOutputStream(tempPath.toFile)
            val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))
            try {
              writer.write(PartitionMetadataFileFormatter.toFile(new PartitionMetadata(CurrentVersion, topicId)))
              writer.flush()
              fileOutputStream.getFD().sync()
            } finally {
              writer.close()
            }

            Utils.atomicMoveWithFallback(tempPath, path)
          } catch {
            case e: IOException =>
              // PartitionMetadataFile调用maybeFlush()写”partition.metadata“文件失败
              val msg = s"Error while writing to partition metadata file ${file.getAbsolutePath}"
              // 一个文件的刷写失败，会导致整个logdir级别下的所有partition纳入到logDirFailureChannel里面,,,,
              logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
              throw new KafkaStorageException(msg, e)
          }
          dirtyTopicIdOpt = None
        }
      }
    }
  }

  def read(): PartitionMetadata = {
    lock synchronized {
      try {
        val reader = Files.newBufferedReader(path)
        try {
          val partitionBuffer = new PartitionMetadataReadBuffer(file.getAbsolutePath, reader)
          partitionBuffer.read()
        } finally {
          reader.close()
        }
      } catch {
        case e: IOException =>
          // PartitionMetadataFile调用 read() 读”partition.metadata“文件失败
          val msg = s"Error while reading partition metadata file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }

  def exists(): Boolean = {
    file.exists()
  }

  def delete(): Unit = {
    Files.delete(file.toPath)
  }

  override def toString: String = s"PartitionMetadataFile(path=$path)"
}
