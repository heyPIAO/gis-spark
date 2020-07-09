package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.SparkSession;

/**
 * @author Hu
 * @date 2020/7/9
 **/
@AllArgsConstructor
public abstract class BinaryOperatorImpl implements BinaryOperator {
  private SparkSession ss;

  @Override
  public Layer run(Feature f, Layer layer) {
    return this.run(f.getGeometry(), layer);
  }

  @Override
  public Layer run(Feature f, IndexedLayer layer) {
    return this.run(f.getGeometry(), layer);
  }
}
