package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;

/**
 * @author Hu
 * @date 2019/9/20
 * 图层类型
 **/
public enum LayerType {

  POINT_LAYER(0, FeatureType.POINT),
  POLYLINE_LAYER(1, FeatureType.POLYLINE),
  POLYGON_LAYER(2, FeatureType.POLYGON),
  TRAJECTORY_POINT_LAYER(3, FeatureType.TRAJECTORY_POINT),
  TRAJECTORY_POLYLINE_LAYER(4, FeatureType.TRAJECTORY_POLYLINE),
  MULTI_POINT_LAYER(5, FeatureType.MULTI_POINT),
  MULTI_POLYLINE_LAYER(6, FeatureType.MULTI_POLYLINE),
  MULTI_POLYGON_LAYER(7, FeatureType.MULTI_POLYGON);

  @Getter
  private int type;

  @Getter
  private FeatureType featureType;


  LayerType(int type, FeatureType featureType) {
    this.type = type;
    this.featureType = featureType;
  }

  public Class<? extends Layer> getLayerClass() {
    return this.getLayerClass(this.featureType);
  }


  public static LayerType findLayerType(FeatureType featureType) {
    for (LayerType lt: values()) {
      if (lt.featureType.equals(featureType)) {
        return lt;
      }
    }
    throw new GISSparkException("Unvliad feature type name: " + featureType.getName());
  }

  private Class<? extends Layer> getLayerClass(FeatureType ft) {
    if (ft.equals(FeatureType.POINT)) return PointLayer.class;
    if (ft.equals(FeatureType.POLYLINE)) return PolylineLayer.class;
    if (ft.equals(FeatureType.POLYGON)) return PolygonLayer.class;
    if (ft.equals(FeatureType.MULTI_POINT)) return MultiPointLayer.class;
    if (ft.equals(FeatureType.MULTI_POLYLINE)) return MultiPolylineLayer.class;
    if (ft.equals(FeatureType.MULTI_POLYGON)) return MultiPolygonLayer.class;
    if (ft.equals(FeatureType.TRAJECTORY_POINT)) return TrajectoryPointLayer.class;
    if (ft.equals(FeatureType.TRAJECTORY_POLYLINE)) return TrajectoryPolylineLayer.class;
    throw new GISSparkException("Unvalid feature type: " + ft.getName());
  }

}
