package org.apache.spark.sql.hls.udf

import java.nio.charset.StandardCharsets

import edu.zju.gis.hls.trajectory.analysis.proto.{TemporalLineString, TemporalPoint}
import org.apache.spark.sql.SQLContext
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}
import org.apache.spark.sql.hls.udf.SQLFunctionHelper._

/**
 *
 * @author Hu
 * @date 2020/10/2
 **/
object GeometricCastFunctions {
  val ST_CastToPoint:      Geometry => Point       = g => g.asInstanceOf[Point]
  val ST_CastToTemporalPoint:      Geometry => Point       = g => g.asInstanceOf[TemporalPoint]
  val ST_CastToPolygon:    Geometry => Polygon     = g => g.asInstanceOf[Polygon]
  val ST_CastToLineString: Geometry => LineString  = g => g.asInstanceOf[LineString]
  val ST_CastToTemporalLineString: Geometry => LineString  = g => g.asInstanceOf[TemporalLineString]
  val ST_CastToGeometry:   Geometry => Geometry    = g => g
  val ST_ByteArray: (String) => Array[Byte] =
    nullableUDF((string) => string.getBytes(StandardCharsets.UTF_8))

  private val castingNames = Map(
    ST_CastToPoint -> "st_castToPoint",
    ST_CastToTemporalPoint -> "st_castToTemporalPoint",
    ST_CastToPolygon -> "st_castToPolygon",
    ST_CastToLineString -> "st_castToLineString",
    ST_CastToTemporalLineString -> "st_castToTemporalLineString",
    ST_CastToGeometry -> "st_castToGeometry",
    ST_ByteArray -> "st_byteArray"
  )

  def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register type casting functions
    sqlContext.udf.register(castingNames(ST_CastToPoint), ST_CastToPoint)
    sqlContext.udf.register(castingNames(ST_CastToTemporalPoint), ST_CastToTemporalPoint)
    sqlContext.udf.register(castingNames(ST_CastToTemporalLineString), ST_CastToTemporalLineString)
    sqlContext.udf.register(castingNames(ST_CastToPolygon), ST_CastToPolygon)
    sqlContext.udf.register(castingNames(ST_CastToLineString), ST_CastToLineString)
    sqlContext.udf.register(castingNames(ST_CastToGeometry), ST_CastToGeometry)
    sqlContext.udf.register(castingNames(ST_ByteArray), ST_ByteArray)
  }
}
