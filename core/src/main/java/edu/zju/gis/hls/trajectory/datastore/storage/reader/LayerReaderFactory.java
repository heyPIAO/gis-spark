package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import edu.zju.gis.hls.trajectory.analysis.model.FeatureType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.*;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hu
 * @date 2019/9/20
 **/
public class LayerReaderFactory {

  private static Logger logger = LoggerFactory.getLogger(LayerReaderFactory.class);

  public static LayerReader getReader(SparkSession ss, FeatureType featureType, SourceType sourceType) {

    try {
      Class c = Class.forName(featureType.getClassName());
      if (sourceType.equals(SourceType.FILE)) {
        LayerType layerType = featureType.getLayerType();
        switch (layerType) {
          case POINT_LAYER:
            return new FileLayerReader<PointLayer> (ss, c);
          case POLYLINE_LAYER:
            return new FileLayerReader<PolylineLayer> (ss, c);
          case POLYGON_LAYER:
            return new FileLayerReader<PolygonLayer> (ss, c);
          case TRAJECTORY_POINT_LAYER:
            return new FileLayerReader<TrajectoryPointLayer> (ss, c);
          case TRAJECTORY_POLYLINR_LAYER:
            return new FileLayerReader<TrajectoryPolylineLayer> (ss, c);
        }
      } else {
        logger.error("Unsupport source type: " + sourceType.name());
      }
    } catch (ClassNotFoundException e) {
      logger.error("Unsupport layer type: " + featureType.name());
    }
    return null;
  }

}
