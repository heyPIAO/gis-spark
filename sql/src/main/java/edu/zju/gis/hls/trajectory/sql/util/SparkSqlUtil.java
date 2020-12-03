package edu.zju.gis.hls.trajectory.sql.util;

import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.gisspark.model.util.SparkUtil;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hls.udf.*;
import org.apache.spark.sql.hls.udt.*;
import org.apache.spark.sql.types.UDTRegistration;
import org.locationtech.jts.geom.*;

/**
 * @author Hu
 * @date 2020/10/1
 **/
public class SparkSqlUtil {

  public static SparkSession createSparkSessionWithSqlExtent(SparkSessionType type, String appName) {
    return createSparkSessionWithSqlExtent(type, appName, new SparkConf());
  }

  public static SparkSession createSparkSessionWithSqlExtent(SparkSessionType type, String appName, SparkConf conf) {
    SparkSession ss = SparkUtil.getSparkSession(type, appName, conf);
    registerTypes();
    registerFunctions(ss.sqlContext());
    return ss;
  }

  private static void registerTypes() {
    UDTRegistration.register(Geometry.class.getCanonicalName(), GeometryUDT.class.getCanonicalName());
    UDTRegistration.register(Point.class.getCanonicalName(), PointUDT.class.getCanonicalName());
    UDTRegistration.register(TemporalPoint.class.getCanonicalName(), TemporalPointUDT.class.getCanonicalName());
    UDTRegistration.register(LineString.class.getCanonicalName(), PolylineUDT.class.getCanonicalName());
    UDTRegistration.register(TemporalLineString.class.getCanonicalName(), TemporalPolylineUDT.class.getCanonicalName());
    UDTRegistration.register(Polygon.class.getCanonicalName(), PolygonUDT.class.getCanonicalName());
    UDTRegistration.register(MultiPoint.class.getCanonicalName(), MultiPointUDT.class.getCanonicalName());
    UDTRegistration.register(MultiLineString.class.getCanonicalName(), MultiPolylineUDT.class.getCanonicalName());
    UDTRegistration.register(MultiPolygon.class.getCanonicalName(), MultiPolygonUDT.class.getCanonicalName());
  }

  private static void registerFunctions(SQLContext sqlContext) {
    GeometricAccessorFunctions.registerFunctions(sqlContext);
    GeometricCastFunctions.registerFunctions(sqlContext);
    GeometricConstructorFunctions.registerFunctions(sqlContext);
    GeometricOutputFunctions.registerFunctions(sqlContext);
    SpatialRelationFunctions.registerFunctions(sqlContext);
  }

}
