package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.*;
import lombok.Getter;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/9/19
 * 系统支持的空间对象类型
 **/
public enum FeatureType implements Serializable {

  POINT("point", PointFeature.class.getName()), POLYLINE("polyline", PolylineFeature.class.getName()), POLYGON("polygon", PolygonFeature.class.getName()),
  TRAJECTORY_POINT("trajectory_point", TrajectoryPoint.class.getName()), TRAJECTORY_POLYLINE("trajectory_polyline", TrajectoryPolyline.class.getName());

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
      case "trajectory_point":
        return LayerType.TRAJECTORY_POINT_LAYER;
      case "trajectory_polyline":
        return LayerType.TRAJECTORY_POLYLINR_LAYER;
    }
    return null;
  }

  public static FeatureType getType(String name) {
    FeatureType featureType;
    if (name.equals(PointFeature.class.getName())){
      featureType = FeatureType.POINT;
    } else if (name.equals(PolylineFeature.class.getName())) {
      featureType = FeatureType.POLYLINE;
    } else if (name.equals(PolygonFeature.class.getName())) {
      featureType = FeatureType.POLYGON;
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
