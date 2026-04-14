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

import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils

/** Utilities for copying Delta tables to output directories. */
object TableCopier {

  /** Copy a Delta table directory to the destination. */
  def copyTable(src: Path, dest: Path): Unit = {
    require(Files.exists(src), s"Source not found: $src")
    if (Files.exists(dest)) FileUtils.deleteDirectory(dest.toFile)
    FileUtils.copyDirectory(src.toFile, dest.toFile)
  }

  /** Clean and recreate an output directory. */
  def cleanOutputDir(dir: Path): Unit = {
    if (Files.exists(dir)) FileUtils.deleteDirectory(dir.toFile)
    Files.createDirectories(dir)
  }
}
