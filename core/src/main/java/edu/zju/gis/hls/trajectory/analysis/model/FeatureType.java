package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/9/19
 * 系统支持的空间对象类型
 **/
public enum FeatureType implements Serializable {

  GEOMETRY("geometry", Geometry.class.getName()),

  POINT("point", Point.class.getName()),
  POLYLINE("polyline", Polyline.class.getName()),
  POLYGON("polygon", Polygon.class.getName()),
  MULTI_POINT("multi_point", MultiPoint.class.getName()),
  MULTI_POLYLINE("multi_polyline", MultiPolyline.class.getName()),
  MULTI_POLYGON("multi_polygon", MultiPolygon.class.getName()),
  TRAJECTORY_POINT("trajectory_point", TimedPoint.class.getName()),
  TRAJECTORY_POLYLINE("trajectory_polyline", MovingPoint.class.getName());

  @Getter
  private String name;

  @Getter
  private String className;

  FeatureType(String name, String className) {
    this.name = name;
    this.className = className;
  }

  public Class<? extends Feature> getClz() {
    Class<? extends Feature> clz = null;
    try {
      clz = (Class<? extends Feature>) Class.forName(className);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new GISSparkException("Must be a sub-class of Feature: " + className);
    }
    // if (!Feature.class.isAssignableFrom(clz))
    return clz;
  }

  public static FeatureType getFeatureType(String name) {
    for (FeatureType ft: values()) {
      if (ft.getClassName().equals(name)) {
        return ft;
      }
    }
    throw new GISSparkException("Unvliad feature type class name: " + name);
  }

}
