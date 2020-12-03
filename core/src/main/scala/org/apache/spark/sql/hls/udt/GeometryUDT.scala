package org.apache.spark.sql.hls.udt

import edu.zju.gis.hls.trajectory.analysis.proto.{TemporalLineString, TemporalPoint}
import edu.zju.gis.hls.trajectory.analysis.util.GeometryUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.locationtech.jts.geom._

import scala.reflect.{ClassTag, classTag}

/**
 * Based on Geomesa AbstractGeometryUDT
 * @author Hu
 **/
private [spark] class AbstractGeometryUDT[T >: Null <: Geometry: ClassTag]
  extends UserDefinedType[T] {

  override def serialize(obj: T): InternalRow = {
    new GenericInternalRow(Array[Any](GeometryUtil.toByteArray(obj)))
  }

  override def sqlType: DataType = StructType(Seq(
    StructField("GEOM",DataTypes.BinaryType)
  ))

  override def userClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def deserialize(datum: Any): T = {
    val ir = datum.asInstanceOf[InternalRow]
    GeometryUtil.fromByteArray(ir.getBinary(0)).asInstanceOf[T]
  }
}

private [spark] class GeometryUDT extends AbstractGeometryUDT[Geometry]
case object GeometryUDT extends GeometryUDT

private [spark] class PointUDT extends AbstractGeometryUDT[Point]
object PointUDT extends PointUDT

private [spark] class MultiPointUDT extends AbstractGeometryUDT[MultiPoint]
object MultiPointUDT extends MultiPointUDT

private [spark] class PolylineUDT extends AbstractGeometryUDT[LineString]
object PolylineUDT extends PolylineUDT

private [spark] class MultiPolylineUDT extends AbstractGeometryUDT[MultiLineString]
object MultiPolylineUDT extends MultiPolylineUDT

private [spark] class PolygonUDT extends AbstractGeometryUDT[Polygon]
object PolygonUDT extends PolygonUDT

private [spark] class MultiPolygonUDT extends AbstractGeometryUDT[MultiPolygon]
object MultiPolygonUDT extends MultiPolygonUDT

private [spark] class TemporalPointUDT extends AbstractGeometryUDT[TemporalPoint]
object TemporalPointUDT extends TemporalPointUDT

private [spark] class TemporalPolylineUDT extends AbstractGeometryUDT[TemporalLineString]
object TemporalPolylineUDT extends TemporalPolylineUDT
