package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Iterator;

/**
 * @author Hu
 * @date 2019/12/16
 * 四叉树索引
 **/
@Getter
@Setter
public class QuadTreeIndex implements SpatialIndex {

  private static final Logger logger = LoggerFactory.getLogger(QuadTreeIndex.class);

  /**
   * 构建四叉树索引
   * @param layer
   * @return
   */
  @Override
  public QuadTreeIndexLayer index(Layer layer) {
    CoordinateReferenceSystem crs = layer.getMetadata().getCrs();
    return null;
  }

  private class QuadTreeIndexBuiler<K, V extends Feature> implements FlatMapFunction<Tuple2<K, V>, Tuple2<String, V>> {

    private PyramidConfig pc;

    @Override
    public Iterator<Tuple2<String, V>> call(Tuple2<K, V> in) throws Exception {
      return null;
    }
  }

}
