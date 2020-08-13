package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import org.apache.spark.sql.SparkSession;

/**
 * 图层单目操作接口实现类
 * @author Keran Sun (katus)
 * @version 1.0, 2020-08-13
 */
public abstract class OperatorImpl implements Operator {
  private SparkSession ss;

  public OperatorImpl(SparkSession ss) {
    this.ss = ss;
  }

  public <T extends KeyIndexedLayer> Layer run(T layer) {
    return this.run(layer.toLayer());
  }
}
