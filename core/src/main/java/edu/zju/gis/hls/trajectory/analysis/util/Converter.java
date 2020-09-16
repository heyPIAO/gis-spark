package edu.zju.gis.hls.trajectory.analysis.util;

import org.locationtech.jts.geom.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/9/21
 * 将字符串转为指定类型
 **/
public class Converter implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(Converter.class);

  public static Object convert(String v, Class<?> c) {

    if (c.equals(String.class)) {
      return v;
    } else if (c.equals(Integer.class) || c.equals(int.class)) {
      return Integer.valueOf(v);
    } else if (c.equals(Double.class) || c.equals(double.class)) {
      return Double.valueOf(v.isEmpty()?"0.0":v);
    } else if (c.equals(Float.class) || c.equals(float.class)) {
      return Float.valueOf(v);
    } else if (c.equals(Long.class) || c.equals(long.class)) {
      return Long.valueOf(v);
    } else {
      logger.error("Unsupport attribute type: " + c.getName());
      return v;
    }

  }

  /**
   * 统一 Multi 图层的 Geometry 类型
   * @param geometry
   * @return
   */
  public static Geometry convertToMulti(Geometry geometry) {
    GeometryFactory gf = new GeometryFactory();
    if (geometry instanceof Point) {
      Point[] ps = new Point[1];
      ps[0] = (Point) geometry;
      return gf.createMultiPoint(ps);
    } else if (geometry instanceof LineString) {
      LineString[] ls = new LineString[1];
      ls[0] = (LineString) geometry;
      return gf.createMultiLineString(ls);
    } else if (geometry instanceof Polygon) {
      Polygon[] pls = new Polygon[1];
      pls[0] = (Polygon) geometry;
      return gf.createMultiPolygon(pls);
    } else {
      return geometry;
    }
  }


}
