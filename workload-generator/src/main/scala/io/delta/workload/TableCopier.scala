/*
 * Copyright (2024) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.workload

import java.nio.file.{Files, Path, StandardCopyOption}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.commons.io.FileUtils

/** Copies a Delta table directory for portable workload output. */
object TableCopier {

  /**
   * Copy a Delta table to the output directory.
   * @param syncTimestamps If true, sync commit file mtimes to commitInfo.timestamp
   */
  def copyTable(sourceTablePath: Path, destTablePath: Path, syncTimestamps: Boolean = false): Unit = {
    require(Files.exists(sourceTablePath), s"Source not found: $sourceTablePath")
    copyDirectory(sourceTablePath, destTablePath)
    if (syncTimestamps) syncCommitFileTimestamps(destTablePath)
  }

  def cleanOutputDir(outputDir: Path): Unit = {
    if (Files.exists(outputDir)) FileUtils.deleteDirectory(outputDir.toFile)
    Files.createDirectories(outputDir)
  }

  /** Recursively copy a directory tree preserving mtimes. */
  private def copyDirectory(src: Path, dest: Path): Unit = {
    if (!Files.exists(src)) return
    if (Files.exists(dest)) FileUtils.deleteDirectory(dest.toFile)
    val stream = Files.walk(src)
    try {
      stream.iterator().asScala.foreach { sourcePath =>
        val destPath = dest.resolve(src.relativize(sourcePath))
        if (Files.isDirectory(sourcePath)) {
          Files.createDirectories(destPath)
          try { Files.setLastModifiedTime(destPath, Files.getLastModifiedTime(sourcePath)) }
          catch { case NonFatal(_) => }
        } else {
          Files.createDirectories(destPath.getParent)
          Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING)
          try { Files.setLastModifiedTime(destPath, Files.getLastModifiedTime(sourcePath)) }
          catch { case NonFatal(_) => }
        }
      }
    } finally {
      stream.close()
    }
  }

  /**
   * Sync each commit file's mtime to its commitInfo.timestamp.
   * Ensures readers using file mtime produce the same timestamps as those
   * reading commitInfo.timestamp from JSON.
   */
  private def syncCommitFileTimestamps(tablePath: Path): Unit = {
    val deltaLogDir = tablePath.resolve("_delta_log")
    if (!Files.exists(deltaLogDir)) return
    val stream = Files.list(deltaLogDir)
    try {
      stream.iterator().asScala
        .filter(p => p.toString.endsWith(".json") && !p.toString.contains("checkpoint"))
        .foreach { commitFile =>
          try {
            val content = new String(Files.readAllBytes(commitFile), "UTF-8")
            content.split("\n").foreach { line =>
              if (line.contains("\"commitInfo\"")) {
                val ci = JsonUtil.mapper.readTree(line).get("commitInfo")
                if (ci != null && ci.has("timestamp")) {
                  Files.setLastModifiedTime(commitFile,
                    java.nio.file.attribute.FileTime.fromMillis(ci.get("timestamp").asLong()))
                }
              }
            }
          } catch { case NonFatal(_) => }
        }
    } finally {
      stream.close()
    }
  }

  /** Delete .crc checksum files for a modified commit JSON file. */
  def invalidateChecksumFilesForModifiedCommit(commitFilePath: Path): Unit = {
    val parent = commitFilePath.getParent
    val fileName = commitFilePath.getFileName.toString
    Files.deleteIfExists(parent.resolve(s"${fileName.stripSuffix(".json")}.crc"))
    Files.deleteIfExists(parent.resolve(s".$fileName.crc"))
  }
}
