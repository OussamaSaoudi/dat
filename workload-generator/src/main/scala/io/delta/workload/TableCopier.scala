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

import java.net.URLDecoder
import java.nio.file.{Files, Path, StandardCopyOption}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.io.FileUtils

/**
 * Handles copying a Delta table directory and fixing up paths for portability.
 *
 * Operations:
 * - Full recursive copy preserving mtimes
 * - Strip test DV file prefixes
 * - Copy DV files referenced by absolute paths
 * - Rewrite absolute paths to relative in commit JSON
 * - Ensure URL-encoded file aliases exist on disk
 * - Sync commit file timestamps to commitInfo.timestamp
 * - Invalidate .crc files after modifying commits
 */
object TableCopier {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /**
   * Copy a Delta table to the output directory and apply all portability fixups.
   *
   * @param sourceTablePath Path to the source Delta table
   * @param destTablePath   Path where the table copy should be written
   * @param syncTimestamps  Whether to sync commit file mtimes to commitInfo.timestamp
   */
  def copyTable(
      sourceTablePath: Path,
      destTablePath: Path,
      syncTimestamps: Boolean = false): Unit = {
    require(Files.exists(sourceTablePath),
      s"Source table path does not exist: $sourceTablePath")

    // 1. Copy the full directory tree
    copyDirectory(sourceTablePath, destTablePath)

    // 2. Copy DV files that may live outside the table directory (tmp/ siblings)
    val srcTmpUnderTable = sourceTablePath.resolve("tmp")
    val srcTmpSibling = Option(sourceTablePath.getParent).map(_.resolve("tmp")).orNull
    for (srcTmp <- Seq(srcTmpUnderTable, srcTmpSibling)
        .filter(p => p != null && Files.exists(p))) {
      val destTmp = destTablePath.resolve("tmp")
      if (!Files.exists(destTmp)) {
        copyDirectory(srcTmp, destTmp)
      }
    }

    // 3. Copy DV files referenced by absolute paths in commit JSON
    copyReferencedDvFilesToDest(sourceTablePath, destTablePath)

    // 4. Rewrite absolute file:// paths in commit to relative
    rewriteAbsolutePathsToRelativeInCommit(destTablePath, sourceTablePath)

    // 5. Ensure URL-encoded file names from log entries exist on disk
    ensureEncodedFileAliasesExist(destTablePath)

    // 6. Strip test DV file prefix (test%dv%prefix-)
    stripTestDvFilePrefix(destTablePath)

    // 7. Sync commit file timestamps if requested (for CDF / time travel)
    if (syncTimestamps) {
      syncCommitFileTimestamps(destTablePath)
    }
  }

  /** Clean and recreate an output directory. */
  def cleanOutputDir(outputDir: Path): Unit = {
    if (Files.exists(outputDir)) {
      FileUtils.deleteDirectory(outputDir.toFile)
    }
    Files.createDirectories(outputDir)
  }

  /** Copy a file preserving its last-modified time. */
  private def copyFilePreserveMtime(src: Path, dest: Path): Unit = {
    Files.createDirectories(dest.getParent)
    Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING)
    try {
      Files.setLastModifiedTime(dest, Files.getLastModifiedTime(src))
    } catch {
      case NonFatal(_) => // best-effort
    }
  }

  /** Recursively copy a directory tree preserving mtimes. */
  def copyDirectory(src: Path, dest: Path): Unit = {
    if (!Files.exists(src)) return
    if (Files.exists(dest)) {
      FileUtils.deleteDirectory(dest.toFile)
    }
    Files.walk(src).iterator().asScala.foreach { sourcePath =>
      val relative = src.relativize(sourcePath)
      val destPath = dest.resolve(relative)
      if (Files.isDirectory(sourcePath)) {
        Files.createDirectories(destPath)
        try {
          Files.setLastModifiedTime(destPath, Files.getLastModifiedTime(sourcePath))
        } catch {
          case NonFatal(_) =>
        }
      } else {
        copyFilePreserveMtime(sourcePath, destPath)
      }
    }
  }

  /**
   * Sync each commit log file's mtime to its commitInfo.timestamp value.
   * This ensures that readers using file mtime for commit timestamps
   * produce the same values as those reading commitInfo.timestamp from JSON.
   */
  def syncCommitFileTimestamps(tablePath: Path): Unit = {
    val deltaLogDir = tablePath.resolve("_delta_log")
    if (!Files.exists(deltaLogDir)) return

    Files.list(deltaLogDir).iterator().asScala
      .filter(p => p.toString.endsWith(".json") && !p.toString.contains("checkpoint"))
      .foreach { commitFile =>
        try {
          val content = new String(Files.readAllBytes(commitFile), "UTF-8")
          content.split("\n").foreach { line =>
            if (line.contains("\"commitInfo\"")) {
              val node = mapper.readTree(line)
              val commitInfo = node.get("commitInfo")
              if (commitInfo != null && commitInfo.has("timestamp")) {
                val tsMillis = commitInfo.get("timestamp").asLong()
                Files.setLastModifiedTime(commitFile,
                  java.nio.file.attribute.FileTime.fromMillis(tsMillis))
              }
            }
          }
        } catch {
          case NonFatal(_) => // best-effort
        }
      }
  }

  /** Delete .crc checksum files that correspond to a modified commit JSON file. */
  def invalidateChecksumFilesForModifiedCommit(commitFilePath: Path): Unit = {
    val parent = commitFilePath.getParent
    val fileName = commitFilePath.getFileName.toString
    val baseName = fileName.stripSuffix(".json")
    Files.deleteIfExists(parent.resolve(s"$baseName.crc"))
    Files.deleteIfExists(parent.resolve(s".$fileName.crc"))
  }

  /**
   * Strip test DV file name prefix from deletion vector .bin files.
   * The test prefix "test%dv%prefix-" is added by test infrastructure but
   * kernel resolves storageType "u" DVs without any prefix.
   */
  private def stripTestDvFilePrefix(tableDir: Path): Unit = {
    val dvPrefix = "test%dv%prefix-"
    def stripInDir(dir: Path): Unit = {
      if (!Files.exists(dir) || !Files.isDirectory(dir)) return
      Files.list(dir).iterator().asScala.foreach { file =>
        val name = file.getFileName.toString
        if (name.startsWith(dvPrefix) && name.endsWith(".bin")) {
          Files.move(file, file.resolveSibling(name.stripPrefix(dvPrefix)))
        } else if (name.startsWith("." + dvPrefix) && name.endsWith(".bin.crc")) {
          Files.move(file, file.resolveSibling("." + name.stripPrefix("." + dvPrefix)))
        }
      }
    }
    stripInDir(tableDir)
    val tmpDir = tableDir.resolve("tmp")
    if (Files.exists(tmpDir)) {
      Files.list(tmpDir).iterator().asScala
        .filter(Files.isDirectory(_))
        .foreach(stripInDir)
    }
  }

  /**
   * Copy DV files referenced by absolute paths from source to dest.
   * DV storage may place files in tmp/ as sibling of table, so copyDirectory alone may miss them.
   */
  private def copyReferencedDvFilesToDest(
      sourceTablePath: Path,
      destTablePath: Path): Unit = {
    val deltaLogDir = sourceTablePath.resolve("_delta_log")
    if (!Files.exists(deltaLogDir)) return

    val copyFailures = scala.collection.mutable.ArrayBuffer.empty[String]
    val srcUri = sourceTablePath.toAbsolutePath.normalize().toUri.toString
    val srcUriPrefix = if (srcUri.endsWith("/")) srcUri else srcUri + "/"
    val srcUriHadoopPrefix =
      if (srcUriPrefix.startsWith("file:///")) "file:/" + srcUriPrefix.drop("file:///".length)
      else srcUriPrefix
    val srcBase = sourceTablePath.toAbsolutePath.normalize().toString
    val srcBaseSlash = if (srcBase.endsWith("/")) srcBase else srcBase + "/"
    val srcParent = sourceTablePath.getParent
    val srcParentStr =
      if (srcParent != null) {
        val s = srcParent.toAbsolutePath.normalize().toString
        if (s.endsWith("/")) s else s + "/"
      } else ""

    def copyDvFromPath(dvPathStr: String): Unit = {
      val relPathFromLog: Option[String] = {
        if (dvPathStr.startsWith(srcUriPrefix)) {
          Some(dvPathStr.substring(srcUriPrefix.length).stripPrefix("/"))
        } else if (dvPathStr.startsWith(srcUriHadoopPrefix)) {
          Some(dvPathStr.substring(srcUriHadoopPrefix.length).stripPrefix("/"))
        } else if (dvPathStr.startsWith("/")) {
          Some(dvPathStr.stripPrefix("/"))
        } else {
          None
        }
      }
      val srcFile = try {
        if (dvPathStr.startsWith("file:")) {
          val uri = new java.net.URI(dvPathStr)
          val raw = if (uri.getRawPath != null) uri.getRawPath else uri.getPath
          if (raw != null && raw.nonEmpty) {
            val pEnc = java.nio.file.Paths.get(raw)
            val pDec = java.nio.file.Paths.get(URLDecoder.decode(raw, "UTF-8"))
            if (Files.exists(pDec)) pDec else if (Files.exists(pEnc)) pEnc else null
          } else null
        } else if (dvPathStr.startsWith("/")) {
          val pEnc = java.nio.file.Paths.get(dvPathStr)
          val pDec = java.nio.file.Paths.get(URLDecoder.decode(dvPathStr, "UTF-8"))
          if (Files.exists(pDec)) pDec else if (Files.exists(pEnc)) pEnc else null
        } else null
      } catch { case NonFatal(_) => null }

      if (srcFile == null || !Files.exists(srcFile)) {
        copyFailures += s"Missing DV source for $dvPathStr"
        return
      }
      val absStr = srcFile.toAbsolutePath.normalize().toString
      val relPath = relPathFromLog.map { p =>
        try { URLDecoder.decode(p, "UTF-8") } catch { case NonFatal(_) => p }
      }.getOrElse {
        if (absStr.startsWith(srcBaseSlash)) absStr.substring(srcBaseSlash.length)
        else if (srcParentStr.nonEmpty && absStr.startsWith(srcParentStr))
          absStr.substring(srcParentStr.length)
        else {
          val tmpIdx = absStr.indexOf("/tmp/")
          if (tmpIdx >= 0) absStr.substring(tmpIdx + 1) else null
        }
      }
      if (relPath != null && relPath.nonEmpty) {
        val destFile = destTablePath.resolve(relPath)
        if (!Files.exists(destFile)) {
          val srcToUse =
            if (Files.exists(srcFile)) srcFile
            else {
              val fromTable = sourceTablePath.resolve(relPath)
              if (Files.exists(fromTable)) fromTable else null
            }
          if (srcToUse != null) {
            try {
              Files.createDirectories(destFile.getParent)
              copyFilePreserveMtime(srcToUse, destFile)
            } catch {
              case NonFatal(e) =>
                copyFailures += s"Could not copy DV file $srcToUse to $destFile: $e"
            }
          }
        }
      }
    }

    try {
      Files.list(deltaLogDir).iterator().asScala
        .filter(p => p.getFileName.toString.endsWith(".json"))
        .foreach { commitFile =>
          try {
            Files.readAllLines(commitFile).asScala.foreach { line =>
              try {
                val node = mapper.readTree(line)
                Option(node.get("add")).foreach { addNode =>
                  Option(addNode.get("deletionVector")).foreach { dvNode =>
                    if (dvNode.has("storageType") &&
                        dvNode.get("storageType").asText() == "p") {
                      val pathField =
                        if (dvNode.has("pathOrInlineDv")) "pathOrInlineDv"
                        else if (dvNode.has("path")) "path"
                        else null
                      if (pathField != null) {
                        copyDvFromPath(dvNode.get(pathField).asText())
                      }
                    }
                  }
                }
              } catch { case NonFatal(_) => }
            }
          } catch {
            case NonFatal(e) =>
              copyFailures += s"Could not scan commit for DV copy $commitFile: $e"
          }
        }
    } catch {
      case NonFatal(e) =>
        copyFailures += s"Could not scan _delta_log for DV copy: $e"
    }
    if (copyFailures.nonEmpty) {
      System.err.println(
        "WARN: Some DV files could not be copied:\n" + copyFailures.distinct.mkString("\n"))
    }
  }

  /**
   * Rewrite absolute file:// paths in commit JSON to relative paths.
   * This makes the copied table portable across filesystems.
   */
  private def rewriteAbsolutePathsToRelativeInCommit(
      destTablePath: Path,
      sourceTablePath: Path): Unit = {
    val deltaLogDir = destTablePath.resolve("_delta_log")
    if (!Files.exists(deltaLogDir)) return

    val sourceUri = sourceTablePath.toAbsolutePath.normalize().toUri.toString
    val srcPrefix = if (sourceUri.endsWith("/")) sourceUri else sourceUri + "/"

    val commitFiles = Files.list(deltaLogDir).iterator().asScala
      .filter(p => p.getFileName.toString.endsWith(".json"))
      .toList

    for (commitFile <- commitFiles) {
      val lines = Files.readAllLines(commitFile).asScala.toBuffer
      var modified = false
      val newLines = lines.map { line =>
        if (line.contains("\"add\"") && line.contains("\"path\"")) {
          try {
            val node = mapper.readTree(line)
            if (node.has("add")) {
              val addNode = node.get("add")
              if (addNode.has("path") && !addNode.has("deletionVector")) {
                val pathVal = addNode.get("path").asText()
                if (pathVal.startsWith("file://") && pathVal.startsWith(srcPrefix)) {
                  val relPath = pathVal.substring(srcPrefix.length).stripPrefix("/")
                  addNode.asInstanceOf[ObjectNode].put("path", relPath)
                  modified = true
                  mapper.writeValueAsString(node)
                } else line
              } else if (addNode.has("path") && addNode.has("deletionVector")) {
                val addObj = addNode.asInstanceOf[ObjectNode]
                var lineModified = false
                val pathVal = addNode.get("path").asText()
                if (pathVal.startsWith("file://") && pathVal.startsWith(srcPrefix)) {
                  val relPath = pathVal.substring(srcPrefix.length).stripPrefix("/")
                  addObj.put("path", relPath)
                  lineModified = true
                }
                val dvNode = addNode.get("deletionVector")
                val dvPathField =
                  if (dvNode.has("pathOrInlineDv")) Some("pathOrInlineDv")
                  else if (dvNode.has("path")) Some("path")
                  else None
                if (dvPathField.isDefined && dvNode.get("storageType").asText() == "p") {
                  val pathField = dvPathField.get
                  val dvPath = dvNode.get(pathField).asText()
                  val destUri = destTablePath.toAbsolutePath.normalize().toUri.toString
                  val destPrefix = if (destUri.endsWith("/")) destUri else destUri + "/"
                  val srcPrefixHadoop =
                    if (srcPrefix.startsWith("file:///"))
                      "file:/" + srcPrefix.drop("file:///".length)
                    else srcPrefix
                  val candidatePath =
                    if (dvPath.startsWith(srcPrefix)) {
                      destPrefix + dvPath.substring(srcPrefix.length).stripPrefix("/")
                    } else if (dvPath.startsWith(srcPrefixHadoop)) {
                      destPrefix + dvPath.substring(srcPrefixHadoop.length).stripPrefix("/")
                    } else if (dvPath.startsWith("/")) {
                      destPrefix + dvPath.stripPrefix("/")
                    } else dvPath
                  if (candidatePath != dvPath) {
                    dvNode.asInstanceOf[ObjectNode].put(pathField, candidatePath)
                    lineModified = true
                  }
                }
                if (lineModified) {
                  modified = true
                  mapper.writeValueAsString(node)
                } else line
              } else line
            } else line
          } catch { case NonFatal(_) => line }
        } else line
      }
      if (modified) {
        Files.write(commitFile, newLines.asJava)
        invalidateChecksumFilesForModifiedCommit(commitFile)
      }
    }
  }

  /**
   * Ensure that both URL-encoded and decoded forms of file paths exist on disk.
   * Delta logs store paths as URI-encoded strings but physical files use decoded names.
   */
  private def ensureEncodedFileAliasesExist(tableDir: Path): Unit = {
    val deltaLogDir = tableDir.resolve("_delta_log")
    if (!Files.exists(deltaLogDir)) return

    val tablePrefixUri = tableDir.toAbsolutePath.normalize().toUri.toString
    val tablePrefixNorm = if (tablePrefixUri.endsWith("/")) tablePrefixUri else tablePrefixUri + "/"
    val tablePrefixHadoop =
      if (tablePrefixNorm.startsWith("file:///"))
        "file:/" + tablePrefixNorm.drop("file:///".length)
      else tablePrefixNorm

    def ensureAliasPair(relPathEncoded: String): Unit = {
      if (!relPathEncoded.contains("%")) return
      val decodedPath = try {
        URLDecoder.decode(relPathEncoded, "UTF-8")
      } catch { case NonFatal(_) => return }
      if (decodedPath == relPathEncoded) return

      val physicalFile = tableDir.resolve(decodedPath)
      val encodedFile = tableDir.resolve(relPathEncoded)
      if (Files.exists(physicalFile) && !Files.exists(encodedFile)) {
        try {
          Files.createDirectories(encodedFile.getParent)
          Files.copy(physicalFile, encodedFile, StandardCopyOption.REPLACE_EXISTING)
        } catch { case NonFatal(_) => }
      } else if (Files.exists(encodedFile) && !Files.exists(physicalFile)) {
        try {
          Files.createDirectories(physicalFile.getParent)
          Files.copy(encodedFile, physicalFile, StandardCopyOption.REPLACE_EXISTING)
        } catch { case NonFatal(_) => }
      }
    }

    try {
      Files.list(deltaLogDir).iterator().asScala
        .filter(p => p.getFileName.toString.endsWith(".json"))
        .foreach { commitFile =>
          try {
            Files.readAllLines(commitFile).asScala.foreach { line =>
              try {
                val node = mapper.readTree(line)
                Option(node.get("add")).foreach { addNode =>
                  Option(addNode.get("path")).foreach { pathNode =>
                    val logPath = pathNode.asText()
                    if (logPath.contains("%") && !logPath.startsWith("file:")) {
                      ensureAliasPair(logPath)
                    }
                  }
                  Option(addNode.get("deletionVector")).foreach { dvNode =>
                    if (dvNode.has("storageType") && dvNode.get("storageType").asText() == "p") {
                      val pathField =
                        if (dvNode.has("pathOrInlineDv")) "pathOrInlineDv"
                        else if (dvNode.has("path")) "path"
                        else null
                      if (pathField != null) {
                        val dvPath = dvNode.get(pathField).asText()
                        if (dvPath.startsWith("file:") && dvPath.contains("%")) {
                          val relEncoded =
                            if (dvPath.startsWith(tablePrefixNorm))
                              dvPath.substring(tablePrefixNorm.length).stripPrefix("/")
                            else if (dvPath.startsWith(tablePrefixHadoop))
                              dvPath.substring(tablePrefixHadoop.length).stripPrefix("/")
                            else null
                          if (relEncoded != null && relEncoded.nonEmpty) {
                            ensureAliasPair(relEncoded)
                          }
                        }
                      }
                    }
                  }
                }
              } catch { case NonFatal(_) => }
            }
          } catch { case NonFatal(_) => }
        }
    } catch { case NonFatal(_) => }
  }
}
