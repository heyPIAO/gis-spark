package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import lombok.Getter;

/**
 * @author Hu
 * @date 2019/9/20
 * 图层类型
 **/
public enum LayerType {

  POINT_LAYER(0, FeatureType.POINT, PointLayer.class.getName()),
  POLYLINE_LAYER(1, FeatureType.POLYLINE, PolylineLayer.class.getName()),
  POLYGON_LAYER(2, FeatureType.POLYGON, PolygonLayer.class.getName()),
  TRAJECTORY_POINT_LAYER(3, FeatureType.TRAJECTORY_POINT, TrajectoryPointLayer.class.getName()),
  TRAJECTORY_POLYLINR_LAYER(4, FeatureType.TRAJECTORY_POLYLINE, TrajectoryPolylineLayer.class.getName());

  @Getter
  private int type;

  @Getter
  private FeatureType featureType;

  @Getter
  private String className;

  LayerType(int type, FeatureType featureType, String className) {
    this.type = type;
    this.featureType = featureType;
    this.className = className;
  }

  public static FeatureType getFeatureType(LayerType type) {
    return type.featureType;
  }

}
