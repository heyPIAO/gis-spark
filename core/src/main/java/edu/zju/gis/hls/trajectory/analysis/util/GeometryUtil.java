package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import org.locationtech.jts.geom.*;

public class GeometryUtil {

   public static <T> T createEmptyGeometry(Class<T> t) {

       GeometryFactory gf = new GeometryFactory();

       if (t.equals(Point.class)) {
           return (T) gf.createPoint();
       } if (t.equals(MultiPoint.class)) {
           return (T) gf.createMultiPoint();
       } if (t.equals(LineString.class)) {
           return (T) gf.createLineString();
       } if (t.equals(MultiLineString.class)) {
           return (T) gf.createMultiLineString();
       } if (t.equals(Polygon.class)) {
           return (T) gf.createPolygon();
       } if (t.equals(MultiPolygon.class)) {
           return (T) gf.createMultiPolygon();
       } else {
           throw new GISSparkException("Unvalid geometry type: " + t.getClass().getName());
       }
   }

}
