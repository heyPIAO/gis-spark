package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.InnerSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

/**
 * @author Hu
 * @date 2019/12/24
 * 每个partition内部的R树索引
 * TODO 待测
 **/
public class InnerRTreeIndex implements InnerSpatialIndex, Serializable {

  private InnerRTreeIndexConfig conf;

  public InnerRTreeIndex() {
    this(new InnerRTreeIndexConfig());
  }

  public InnerRTreeIndex(InnerRTreeIndexConfig conf) {
    this.conf = conf;
  }

  /**
   * 在每个分片内部构建R-Tree索引
   * @param layer
   * @return
   */
  @Override
  public InnerRTreeIndexLayer index(KeyIndexedLayer layer) {
    return this.indexRTree(layer);
  }

  private InnerRTreeIndexLayer indexRTree(KeyIndexedLayer layer) {
    Layer l = layer.getLayer();
    InnerRTreeIndexLayer result = new InnerRTreeIndexLayer();
    result.setLayer(layer);
    result.setIndexedPartition(l.mapPartitionsToPair(new RTreeIndexBuilder()));
    return result;
  }

  @Getter
  @Setter
  private class RTreeIndexBuilder<V extends Feature> implements PairFlatMapFunction<Iterator<Tuple2<String, V>>, String, RTree> {

    @Override
    public Iterator<Tuple2<String, RTree>> call(Iterator<Tuple2<String, V>> t) throws Exception {

      List<Tuple2<String, RTree>> result = new ArrayList<>();
      String gridId;
      RTree rTree;
      if (t.hasNext()) {
        Tuple2<String, V> m = t.next();
        gridId = m._1;
        Class c = m._2.getClass();
//        LayerType lt = LayerType.findLayerType(FeatureType.getFeatureType(c.getName()));
        rTree = new RTree(1000);
        rTree.insert(m._2.getGeometry().getEnvelopeInternal(), m._1, m._2);
      } else {
        return result.iterator();
      }

      while(t.hasNext()) {
        Tuple2<String, V> m = t.next();
        rTree.insert(m._2.getGeometry().getEnvelopeInternal(), m._1, m._2);
      }
      result.add(new Tuple2<>(gridId, rTree));
      return result.iterator();
    }
  }

}
