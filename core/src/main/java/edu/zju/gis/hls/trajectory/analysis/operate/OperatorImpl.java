package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.AllArgsConstructor;
import org.apache.spark.sql.SparkSession;

/**
 * @author Hu
 * @date 2020/7/9
 **/
@AllArgsConstructor
public abstract class OperatorImpl implements Operator {
  private SparkSession ss;

  public <T extends KeyIndexedLayer> Layer operate(T layer) {
    return this.operate(layer.toLayer());
  }
}
