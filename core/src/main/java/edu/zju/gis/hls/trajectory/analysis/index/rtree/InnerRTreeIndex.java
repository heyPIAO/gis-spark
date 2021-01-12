package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.InnerSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

/**
 * @author Hu
 * @date 2019/12/24
 * 每个 partition 内部的 R树索引
 * TODO 待测
 **/
@Getter
public class InnerRTreeIndex implements InnerSpatialIndex, Serializable {

  private InnerRTreeIndexConfig conf;

  public InnerRTreeIndex() {
    this(new InnerRTreeIndexConfig());
  }

  public InnerRTreeIndex(int nodeCapacity) {
    this(new InnerRTreeIndexConfig(nodeCapacity));
  }

  public InnerRTreeIndex(int nodeCapacity, CoordinateReferenceSystem crs) {
    this(new InnerRTreeIndexConfig(nodeCapacity, crs));
  }

  public InnerRTreeIndex(InnerRTreeIndexConfig conf) {
    this.conf = conf;
  }

  /**
   * 在每个分片内部构建 R-Tree 索引
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
    result.setIndexedPartition(l.mapPartitionsToPair(new InnerRTreeIndexBuilder(this.conf)));
    return result;
  }

  @Getter
  private class InnerRTreeIndexBuilder<V extends Feature> implements PairFlatMapFunction<Iterator<Tuple2<String, V>>, String, RTree> {

    private InnerRTreeIndexConfig conf;

    public InnerRTreeIndexBuilder(InnerRTreeIndexConfig conf) {
      this.conf = conf;
    }

    @Override
    public Iterator<Tuple2<String, RTree>> call(Iterator<Tuple2<String, V>> t) throws Exception {
      List<Tuple2<String, RTree>> result = new ArrayList<>();
      Map<String, RTree> resultMap = new HashMap<>();
      while(t.hasNext()) {
        Tuple2<String, V> m = t.next();
        String gridId = m._1;
        RTree rTree = resultMap.get(gridId);
        if (rTree == null) {
          rTree = new RTree(this.conf.getNodeCapacity(), this.conf.getCrs());
          rTree.insert(m._2.getGeometry().getEnvelopeInternal(), m._1, m._2);
          resultMap.put(gridId, rTree);
        } else {
          rTree.insert(m._2.getGeometry().getEnvelopeInternal(), m._1, m._2);
        }
      }
      for (String key: resultMap.keySet()) {
        result.add(new Tuple2<>(key, resultMap.get(key)));
      }
      return result.iterator();
    }
  }

}
