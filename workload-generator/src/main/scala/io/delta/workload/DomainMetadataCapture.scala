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
import org.apache.spark.sql.delta.actions.DomainMetadata

object DomainMetadataCapture {

  def capture(
      spark: SparkSession, testId: String, tablePath: Path, specsDir: Path,
      domain: String, configuration: String, removed: Boolean = false,
      version: Option[Long] = None, name: String): Unit = {

    val specName = s"${testId}_$name"
    val specPath = specsDir.resolve(s"$specName.json")

    val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
    val snapshot = JsonUtil.resolveSnapshot(deltaLog, version, None)

    val domainActions = snapshot.domainMetadata
    val matchingDomain = domainActions.find(_.domain == domain)

    if (removed) {
      require(matchingDomain.isEmpty,
        s"Domain '$domain' should be removed but is still present")
    } else {
      require(matchingDomain.isDefined,
        s"Domain '$domain' not found in snapshot")
      val dm = matchingDomain.get
      require(dm.configuration == configuration,
        s"configuration mismatch: expected='$configuration' actual='${dm.configuration}'")
    }

    val expected = Some(DomainMetadataExpected(domain, configuration, removed))
    val spec = DomainMetadataSpec(version, expected, None)
    JsonUtil.writeSpec(specPath, spec)
    validateFromSpec(spark, tablePath, specPath)

    val status = if (removed) "removed" else "present"
    println(s"  DomainMetadata captured: $specName (domain=$domain, $status)")
  }

  private[workload] def validateFromSpec(spark: SparkSession, tablePath: Path, specPath: Path): Unit = {
    val spec = JsonUtil.readDomainMetadataSpec(specPath)
    val specName = specPath.getFileName.toString.stripSuffix(".json")

    (spec.expected, spec.expectedError) match {
      case (Some(exp), _) =>
        val deltaLog = DeltaLog.forTable(spark, tablePath.toString)
        val snapshot = JsonUtil.resolveSnapshot(deltaLog, spec.version, None)
        val matchingDomain = snapshot.domainMetadata.find(_.domain == exp.domain)

        if (exp.removed) {
          require(matchingDomain.isEmpty,
            s"Validation FAILED for $specName: domain '${exp.domain}' should be removed")
        } else {
          require(matchingDomain.isDefined,
            s"Validation FAILED for $specName: domain '${exp.domain}' not found")
          require(matchingDomain.get.configuration == exp.configuration,
            s"Validation FAILED for $specName: configuration mismatch")
        }

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
