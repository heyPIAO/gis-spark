package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import lombok.Getter;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/9/19
 * 系统支持的空间对象类型
 **/
public enum FeatureType implements Serializable {

  POINT("point", Point.class.getName()),
  POLYLINE("polyline", Polyline.class.getName()),
  POLYGON("polygon", Polygon.class.getName()),
  MULTI_POINT("multi_point", MultiPoint.class.getName()),
  MULTI_POLYLINE("multi_polyline", MultiPolyline.class.getName()),
  MULTI_POLYGON("multi_polygon", MultiPolygon.class.getName()),
  TRAJECTORY_POINT("trajectory_point", TrajectoryPoint.class.getName()),
  TRAJECTORY_POLYLINE("trajectory_polyline", TrajectoryPolyline.class.getName());

  @Getter
  private String name;

  @Getter
  private String className;

  FeatureType(String name, String className) {
    this.name = name;
    this.className = className;
  }

  public LayerType getLayerType() {
    switch (this.name) {
      case "point":
        return LayerType.POINT_LAYER;
      case "polyline":
        return LayerType.POLYLINE_LAYER;
      case "polygon":
        return LayerType.POLYGON_LAYER;
      case "multi_point":
        return LayerType.MULTI_POINT_LAYER;
      case "multi_polyline":
        return LayerType.MULTI_POLYLINE_LAYER;
      case "multi_polygon":
        return LayerType.MULTI_POLYGON_LAYER;
      case "trajectory_point":
        return LayerType.TRAJECTORY_POINT_LAYER;
      case "trajectory_polyline":
        return LayerType.TRAJECTORY_POLYLINE_LAYER;
    }
    return null;
  }

  public static FeatureType getType(String name) {
    FeatureType featureType;
    if (name.equals(Point.class.getName())){
      featureType = FeatureType.POINT;
    } else if (name.equals(Polyline.class.getName())) {
      featureType = FeatureType.POLYLINE;
    } else if (name.equals(Polygon.class.getName())) {
      featureType = FeatureType.POLYGON;
    } else if (name.equals(MultiPoint.class.getName())) {
      featureType = FeatureType.MULTI_POINT;
    } else if (name.equals(MultiPolyline.class.getName())) {
      featureType = FeatureType.MULTI_POLYLINE;
    } else if (name.equals(MultiPolygon.class.getName())) {
      featureType = FeatureType.MULTI_POLYGON;
    } else if (name.equals(TrajectoryPoint.class.getName())) {
      featureType = FeatureType.TRAJECTORY_POINT;
    } else if (name.equals(TrajectoryPolyline.class.getName())) {
      featureType = FeatureType.TRAJECTORY_POLYLINE;
    } else {
      featureType = null;
    }
    return featureType;
  }

}
