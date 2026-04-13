/*
 * Copyright (2025) The Delta Lake Project Authors.
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

/** Moves (or copies) a Delta table directory for portable workload output. */
object TableCopier {

  /**
   * Move a Delta table to the output directory. Falls back to copy if move
   * fails (e.g., cross-filesystem). The source table is consumed — the caller
   * should not read from sourceTablePath after this call.
   */
  def copyTable(sourceTablePath: Path, destTablePath: Path): Unit = {
    require(Files.exists(sourceTablePath), s"Source not found: $sourceTablePath")
    if (Files.exists(destTablePath)) FileUtils.deleteDirectory(destTablePath.toFile)
    try {
      Files.move(sourceTablePath, destTablePath)
    } catch {
      case _: java.io.IOException =>
        // Cross-filesystem or other move failure — fall back to copy
        copyDirectory(sourceTablePath, destTablePath)
    }
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
}
