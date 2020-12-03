package org.apache.spark.sql.hls.udf

import edu.zju.gis.hls.trajectory.analysis.util.GeometryUtil
import org.apache.spark.sql.SQLContext
import org.locationtech.jts.geom.{Geometry, Point}

import org.apache.spark.sql.hls.udf.SQLFunctionHelper._

/**
 *
 * @author Hu
 * @date 2020/10/2
 **/
object GeometricOutputFunctions {

  val ST_AsBinary: Geometry => Array[Byte] = nullableUDF(geom => GeometryUtil.toWKB(geom))
  val ST_AsLatLonText: Point => String = nullableUDF(point => toLatLonString(point))
  val ST_AsText: Geometry => String = nullableUDF(geom => geom.toText)

  private val outputNames = Map(
    ST_AsBinary -> "st_asBinary",
    ST_AsLatLonText -> "st_asLatLonText",
    ST_AsText -> "st_asText"
  )

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(outputNames(ST_AsBinary), ST_AsBinary)
    sqlContext.udf.register(outputNames(ST_AsLatLonText), ST_AsLatLonText)
    sqlContext.udf.register(outputNames(ST_AsText), ST_AsText)
  }

  private def toLatLonString(point: Point): String = {
    val coordinate = point.getCoordinate
    s"${latLonFormat(coordinate.y, lat = true)} ${latLonFormat(coordinate.x, lat = false)}"
  }

  private def latLonFormat(value: Double, lat: Boolean): String = {
    val degrees = value.floor
    val decimal = value - degrees
    val minutes = (decimal * 60).floor
    val seconds = (decimal * 60 - minutes) * 60
    if (lat)
      f"${degrees.abs}%1.0f\u00B0$minutes%1.0f" +"\'" + f"$seconds%1.3f" + "\"" + s"${if (degrees < 0) "S" else "N"}"
    else
      f"${degrees.abs}%1.0f\u00B0$minutes%1.0f" +"\'" + f"$seconds%1.3f" + "\"" + s"${if (degrees < 0) "W" else "E"}"
  }
}
