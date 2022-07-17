package org.apache.spark.sql.hls


import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.types.StructType


/**
 * Utils for handling schemas.
 *
 * TODO: Merge this file with [[org.apache.spark.ml.util.SchemaUtils]].
 */
private[spark] object SchemaUtils {

  /**
   * Checks if an input schema has duplicate column names. This throws an exception if the
   * duplication exists.
   *
   * @param schema schema to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkSchemaColumnNameDuplication(
                                        schema: StructType, colType: String, caseSensitiveAnalysis: Boolean = false): Unit = {
    checkColumnNameDuplication(schema.map(_.name), colType, caseSensitiveAnalysis)
  }

  // Returns true if a given resolver is case-sensitive
  private def isCaseSensitiveAnalysis(resolver: Resolver): Boolean = {
    if (resolver == caseSensitiveResolution) {
      true
    } else if (resolver == caseInsensitiveResolution) {
      false
    } else {
      sys.error("A resolver to check if two identifiers are equal must be " +
        "`caseSensitiveResolution` or `caseInsensitiveResolution` in o.a.s.sql.catalyst.")
    }
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param resolver resolver used to determine if two identifiers are equal
   */
  def checkColumnNameDuplication(
                                  columnNames: Seq[String], colType: String, resolver: Resolver): Unit = {
    checkColumnNameDuplication(columnNames, colType, isCaseSensitiveAnalysis(resolver))
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkColumnNameDuplication(
                                  columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    val names = if (caseSensitiveAnalysis) columnNames else columnNames.map(_.toLowerCase)
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      throw new AnalysisException(
        s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}")
    }
  }
}