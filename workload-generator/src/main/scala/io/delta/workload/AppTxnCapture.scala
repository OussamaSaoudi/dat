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

import java.nio.file.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

/**
 * Captures and validates application transaction (SetTransaction) specs.
 *
 * Uses DeltaLog APIs to scan for SetTransaction actions, scoped to a
 * maximum version when specified.
 */
object AppTxnCapture {

  /**
   * Capture an appTxn spec: verify that the expected SetTransaction exists in
   * the log and write the spec JSON.
   *
   * @param version If specified, only scan commits up to (and including) this version.
   */
  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      appId: String, expectedTxnVersion: Long,
      version: Option[Long] = None, name: String): Unit = {

    val specName = s"${testId}_$name"
    val specPath = specsDir.resolve(s"$specName.json")

    DeltaLog.clearCache()
    val foundTxn = scanTxnFromLog(spark, tablePath, appId, version)

    require(foundTxn.isDefined,
      s"SetTransaction for appId='$appId' not found in delta log")
    require(foundTxn.get == expectedTxnVersion,
      s"appId='$appId' version expected=$expectedTxnVersion actual=${foundTxn.get}")

    val expected = Some(TxnExpected(appId, expectedTxnVersion))
    val spec = TxnSpec(version, expected, None)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, specPath)

    println(s"  AppTxn captured: $specName (appId=$appId, version=$expectedTxnVersion)")
  }

  /**
   * Scan the Delta log via DeltaLog APIs for a SetTransaction with the given appId.
   * If maxVersion is specified, only commits up to that version are considered.
   *
   * Returns the txn version for the given appId, or None if not found.
   */
  private[workload] def scanTxnFromLog(
      spark: SparkSession,
      tablePath: Path,
      appId: String,
      maxVersion: Option[Long]): Option[Long] = {
    DeltaLog.clearCache()
    val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
    val targetVersion = maxVersion.getOrElse(deltaLog.update().version)
    val snapshot = deltaLog.getSnapshotAt(targetVersion)

    val txns = snapshot.transactions
    txns.get(appId)
  }

  private[workload] def validateFromSpec(
      spark: SparkSession, tablePath: Path, specPath: Path): Unit = {
    DeltaLog.clearCache()
    val spec = JsonUtil.readTxnSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")

    (spec.expected, spec.expectedError) match {
      case (Some(exp), _) =>
        val foundTxn = scanTxnFromLog(spark, tablePath, exp.appId, spec.version)
        require(foundTxn.isDefined,
          s"Validation FAILED for $specName: appId='${exp.appId}' not found")
        require(foundTxn.get == exp.txnVersion,
          s"Validation FAILED for $specName: expected version=${exp.txnVersion}, actual=${foundTxn.get}")

      case (_, Some(_)) =>
        ()

      case _ =>
    }
  }
}
