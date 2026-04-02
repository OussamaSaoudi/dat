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

import java.nio.file.{Files, Path}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Shared utilities for JSON, DataFrame comparison, and column handling. */
object JsonUtil {

  /** Single shared ObjectMapper — use this instead of creating per-file instances. */
  private[workload] val mapper: ObjectMapper =
    new ObjectMapper().registerModule(DefaultScalaModule)

  /** Write an object as pretty-printed JSON. */
  def writeJson(path: Path, data: Any): Unit = {
    val json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(data)
    Files.write(path, json.getBytes("UTF-8"))
  }

  /**
   * Convert a DataFrame to a multiset of canonical JSON rows.
   * Order-independent, handles all types (binary, nested structs, maps, arrays).
   */
  def toRowMultiset(df: DataFrame): Map[String, Int] = {
    df.toJSON.collect().toSeq.groupBy(identity).map {
      case (value, rows) => value -> rows.size
    }
  }

  /** Create a column reference, escaping backticks. Handles _metadata.* references. */
  def columnRef(name: String): Column = {
    if (name.startsWith("_metadata.")) col(name)
    else { val escaped = name.replace("`", "``"); col(s"`$escaped`") }
  }

  /**
   * Build a Delta reader with optional time-travel parameters.
   * Returns a DataFrame ready for predicate/column filtering.
   */
  def buildDeltaReader(
      spark: SparkSession,
      tablePath: Path,
      version: Option[Long],
      timestamp: Option[String]): DataFrame = {
    var reader = spark.read.format("delta")
    version.foreach(v => reader = reader.option("versionAsOf", v))
    timestamp.foreach(ts => reader = reader.option("timestampAsOf", ts))
    reader.load(tablePath.toString)
  }

  /** Apply optional predicate and column projection to a DataFrame. */
  def applyFilters(
      df: DataFrame,
      predicate: Option[String],
      columns: Option[Seq[String]]): DataFrame = {
    var result = df
    predicate.foreach(p => result = result.filter(p))
    columns.foreach(cols => result = result.select(cols.map(columnRef): _*))
    result
  }

  /**
   * Extract error code from an exception.
   * Uses SparkThrowable.getErrorClass if available, otherwise class name.
   */
  def extractErrorCode(e: Exception): String = e match {
    case st: org.apache.spark.SparkThrowable =>
      Option(st.getErrorClass).getOrElse(e.getClass.getSimpleName)
    case _ => e.getClass.getSimpleName
  }

  /** Assert two row multisets are equal, with detailed error reporting on mismatch. */
  def assertMultisetsEqual(
      expected: Map[String, Int],
      actual: Map[String, Int],
      specName: String): Unit = {
    if (expected != actual) {
      val missing = expected.keySet -- actual.keySet
      val extra = actual.keySet -- expected.keySet
      val countMismatches = (expected.keySet & actual.keySet).filter(k =>
        expected(k) != actual(k))
      val details = new StringBuilder()
      if (missing.nonEmpty) {
        details.append(s"\n  Missing rows: ${missing.size}")
        missing.take(3).foreach(r => details.append(s"\n    $r"))
      }
      if (extra.nonEmpty) {
        details.append(s"\n  Extra rows: ${extra.size}")
        extra.take(3).foreach(r => details.append(s"\n    $r"))
      }
      if (countMismatches.nonEmpty) {
        details.append(s"\n  Count mismatches: ${countMismatches.size}")
        countMismatches.take(3).foreach { r =>
          details.append(s"\n    expected ${expected(r)}x, got ${actual(r)}x: $r")
        }
      }
      throw new RuntimeException(
        s"Validation FAILED for $specName: row-level mismatch" +
          s" (expected ${expected.values.sum}, got ${actual.values.sum})$details")
    }
  }
}
