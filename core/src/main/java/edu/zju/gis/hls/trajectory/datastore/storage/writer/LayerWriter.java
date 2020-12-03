package edu.zju.gis.hls.trajectory.datastore.storage.writer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/9/20
 * 图层写出,T 为写出后端的基础逻辑对象
 **/
public abstract class LayerWriter <T> implements Serializable {

  @Getter
  transient protected SparkSession ss;

  @Getter
  transient protected JavaSparkContext jsc;

  public LayerWriter(SparkSession ss) {
    this.ss = ss;
    this.jsc = JavaSparkContext.fromSparkContext(this.ss.sparkContext());
  }


  /**
   * 数据单元转换方法
   * @return
   */
  public abstract T transform(Feature feature);

  /**
   * 图层写出
   * @param layer
   */
  public abstract void write(Layer layer);

  public void write(IndexedLayer layer) {
    this.write(layer.toLayer());
  }

}
