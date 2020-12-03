package org.apache.spark.sql.hls.udf

import edu.zju.gis.hls.trajectory.analysis.util.GeometryUtil
import org.apache.spark.sql.SQLContext
import org.locationtech.jts.geom._
import org.apache.spark.sql.hls.udf.SQLFunctionHelper._

/**
 *
 * @author Hu
 * @date 2020/10/2
 **/
object GeometricConstructorFunctions {

  @transient
  private val geomFactory: GeometryFactory = new GeometryFactory()

  val ST_GeomFromWKT: String => Geometry = nullableUDF(text => GeometryUtil.fromWKT(text))
  val ST_GeomFromWKB: Array[Byte] => Geometry = nullableUDF(array => GeometryUtil.fromWKB(array))
  val ST_LineFromText: String => LineString = nullableUDF(text => GeometryUtil.fromWKT(text).asInstanceOf[LineString])
  val ST_MakeBox2D: (Point, Point) => Geometry = nullableUDF((lowerLeft, upperRight) =>
    geomFactory.toGeometry(new Envelope(lowerLeft.getX, upperRight.getX, lowerLeft.getY, upperRight.getY)))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Geometry = nullableUDF((xmin, xmax, ymin, ymax) =>
    GeometryUtil.createBBox(xmin, xmax, ymin, ymax))
  val ST_MakePolygon: LineString => Polygon = nullableUDF(shell => {
    val ring = geomFactory.createLinearRing(shell.getCoordinateSequence)
    geomFactory.createPolygon(ring)
  })
  val ST_MakePoint: (Double, Double) => Point = nullableUDF((x, y) => geomFactory.createPoint(new Coordinate(x, y)))
  val ST_MakeLine: Seq[Point] => LineString = nullableUDF(s => geomFactory.createLineString(s.map(_.getCoordinate).toArray))
  val ST_MakePointM: (Double, Double, Double) => Point = nullableUDF((x, y, m) =>
    GeometryUtil.fromWKT(s"POINT($x $y $m)").asInstanceOf[Point])
  val ST_MLineFromText: String => MultiLineString = nullableUDF(text => GeometryUtil.fromWKT(text).asInstanceOf[MultiLineString])
  val ST_MPointFromText: String => MultiPoint = nullableUDF(text => GeometryUtil.fromWKT(text).asInstanceOf[MultiPoint])
  val ST_MPolyFromText: String => MultiPolygon = nullableUDF(text => GeometryUtil.fromWKT(text).asInstanceOf[MultiPolygon])
  val ST_Point: (Double, Double) => Point = (x, y) => ST_MakePoint(x, y)
  val ST_PointFromText: String => Point = nullableUDF(text => GeometryUtil.fromWKT(text).asInstanceOf[Point])
  val ST_PointFromWKB: Array[Byte] => Point = array => ST_GeomFromWKB(array).asInstanceOf[Point]
  val ST_Polygon: LineString => Polygon = shell => ST_MakePolygon(shell)
  val ST_PolygonFromText: String => Polygon = nullableUDF(text => GeometryUtil.fromWKT(text).asInstanceOf[Polygon])

  private val constructorNames = Map(
    ST_GeomFromWKT -> "st_geomFromWKT",
    ST_GeomFromWKB -> "st_geomFromWKB",
    ST_LineFromText -> "st_lineFromText",
    ST_MakeBox2D -> "st_makeBox2D",
    ST_MakeBBOX -> "st_makeBBOX",
    ST_MakePolygon -> "st_makePolygon",
    ST_MakePoint -> "st_makePoint",
    ST_MakeLine -> "st_makeLine",
    ST_MakePointM -> "st_makePointM",
    ST_MLineFromText -> "st_mLineFromText",
    ST_MPointFromText -> "st_mPointFromText",
    ST_MPolyFromText -> "st_mPolyFromText",
    ST_Point -> "st_point",
    ST_PointFromText -> "st_pointFromText",
    ST_PointFromWKB -> "st_pointFromWKB",
    ST_Polygon -> "st_polygon",
    ST_PolygonFromText  -> "st_polygonFromText"
  )

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_geomFromText", ST_GeomFromWKT)
    sqlContext.udf.register("st_geometryFromText", ST_GeomFromWKT)
    sqlContext.udf.register(constructorNames(ST_GeomFromWKT), ST_GeomFromWKT)
    sqlContext.udf.register(constructorNames(ST_GeomFromWKB), ST_GeomFromWKB)
    sqlContext.udf.register(constructorNames(ST_LineFromText), ST_LineFromText)
    sqlContext.udf.register(constructorNames(ST_MLineFromText), ST_MLineFromText)
    sqlContext.udf.register(constructorNames(ST_MPointFromText), ST_MPointFromText)
    sqlContext.udf.register(constructorNames(ST_MPolyFromText), ST_MPolyFromText)
    sqlContext.udf.register(constructorNames(ST_MakeBBOX), ST_MakeBBOX)
    sqlContext.udf.register(constructorNames(ST_MakeBox2D), ST_MakeBox2D)
    sqlContext.udf.register(constructorNames(ST_MakeLine), ST_MakeLine)
    sqlContext.udf.register(constructorNames(ST_MakePoint), ST_MakePoint)
    sqlContext.udf.register(constructorNames(ST_MakePointM), ST_MakePointM)
    sqlContext.udf.register(constructorNames(ST_MakePolygon), ST_MakePolygon)
    sqlContext.udf.register(constructorNames(ST_Point), ST_Point)
    sqlContext.udf.register(constructorNames(ST_PointFromText), ST_PointFromText)
    sqlContext.udf.register(constructorNames(ST_PointFromWKB), ST_PointFromWKB)
    sqlContext.udf.register(constructorNames(ST_Polygon), ST_Polygon)
    sqlContext.udf.register(constructorNames(ST_PolygonFromText), ST_PolygonFromText)
  }
}
