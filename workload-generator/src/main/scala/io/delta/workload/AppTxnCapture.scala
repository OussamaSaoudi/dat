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

object AppTxnCapture {

  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      appId: String, expectedTxnVersion: Long,
      version: Option[Long] = None, name: String): Unit = {

    val specName = s"${testId}_$name"
    val specPath = specsDir.resolve(s"$specName.json")

    val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = JsonUtil.resolveSnapshot(deltaLog, version, None)

    val txnMap = snapshot.transactions
    val actualVersion = txnMap.get(appId)

    require(actualVersion.isDefined,
      s"SetTransaction for appId '$appId' not found in snapshot")
    require(actualVersion.get == expectedTxnVersion,
      s"SetTransaction version mismatch for appId '$appId': " +
        s"expected=$expectedTxnVersion actual=${actualVersion.get}")

    val expected = Some(TxnExpected(appId, expectedTxnVersion))
    val spec = TxnSpec(version, expected, None)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, specPath)

    println(s"  AppTxn captured: $specName (appId=$appId, version=$expectedTxnVersion)")
  }

  private[workload] def validateFromSpec(spark: SparkSession, tablePath: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readTxnSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")

    (spec.expected, spec.expectedError) match {
      case (Some(exp), _) =>
        val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
        val snapshot = JsonUtil.resolveSnapshot(deltaLog, spec.version, None)
        val txnMap = snapshot.transactions
        val actualVersion = txnMap.get(exp.appId)

        require(actualVersion.isDefined,
          s"Validation FAILED for $specName: appId '${exp.appId}' not found")
        require(actualVersion.get == exp.txnVersion,
          s"Validation FAILED for $specName: txnVersion mismatch " +
            s"(expected=${exp.txnVersion}, actual=${actualVersion.get})")

      case (_, Some(err)) =>
        val actualCode = try {
          val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
          JsonUtil.resolveSnapshot(deltaLog, spec.version, None)
          None
        } catch {
          case e: Exception => Some(JsonUtil.extractErrorCode(e))
        }
        require(actualCode.isDefined,
          s"Error validation FAILED for $specName: expected operation to fail but it succeeded")
        if (actualCode.get != err.errorCode) {
          System.err.println(s"WARN: Error code mismatch for $specName: " +
            s"captured '${err.errorCode}' but got '${actualCode.get}'")
        }

      case _ =>
    }
  }
}
