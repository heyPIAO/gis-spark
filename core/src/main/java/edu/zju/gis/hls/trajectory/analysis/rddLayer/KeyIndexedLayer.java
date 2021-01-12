package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.index.partitioner.SpatialPartitioner;
import edu.zju.gis.hls.trajectory.analysis.index.rtree.RTree;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Hu
 * @date 2019/12/16
 * 基于 Key 构建索引，用于数据分区
 **/
@Slf4j
@NoArgsConstructor
public class KeyIndexedLayer<L extends Layer> extends IndexedLayer<L> {

  @Getter
  @Setter
  protected L layer;

  @Getter
  @Setter
  protected SpatialPartitioner partitioner;

  public KeyIndexedLayer(SpatialPartitioner partitioner) {
    this.partitioner = partitioner;
  }

  public LayerMetadata getMetadata() {
    return this.layer.metadata;
  }

  public Field[] getAttributes() {
    return this.layer.metadata.getAttributes().keySet().toArray(new Field[]{});
  }

  @Override
  public L toLayer() {
    return this.layer;
  }

  @Override
  public KeyIndexedLayer<L> query(Geometry geometry) {
    List<String> partitionKeys = this.queryPartitionsKeys(geometry);
    L t = (L) this.layer.filterToLayer(new Function<Tuple2<String, Feature>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, Feature> x) throws Exception {
        return partitionKeys.contains(x._1);
      }
    }).filterToLayer(new Function<Tuple2<String, Feature>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, Feature> in) throws Exception {
        return geometry.contains(in._2.getGeometry()) || geometry.intersects(in._2.getGeometry());
      }
    });
    return new KeyIndexedLayer<>(t, this);
  }

  public KeyIndexedLayer(L layer, KeyIndexedLayer<L> l) {
    this.layer = layer;
    this.partitioner = l.getPartitioner();
    this.indexType = l.getIndexType();
  }

  public List<String> queryPartitionsKeys(Geometry geometry) {
    return this.partitioner.getKey(geometry);
  }

  public KeyIndexedLayer<L> intersect(KeyIndexedLayer layer2, Boolean attrReserved) {
    if (!this.getPartitioner().equals(layer2.getPartitioner())) {
      throw new GISSparkException("two layer mush have the same partitioner with the same key");
    }

    // 原理上，相同的分区器的KeyIndexLayer，相同的key存储在同一台机子上，利用相同key做join可减少shuffle
    // 仅关联有相同 key 的元祖，不做 full outer join
    JavaPairRDD<String, Tuple2<Iterable<Feature>, Iterable<Feature>>> cogroup = this.getLayer().cogroup(layer2.getLayer());
    JavaPairRDD<String, Feature> res = cogroup.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<Feature>, Iterable<Feature>>>, String, Feature>() {
      @Override
      public Iterator<Tuple2<String, Feature>> call(Tuple2<String, Tuple2<Iterable<Feature>, Iterable<Feature>>> input) throws Exception {
        Iterator<Feature> in1 = input._2._1.iterator();
        List<Feature> in2= IteratorUtils.toList(input._2._2.iterator());
        List<Tuple2<String, Feature>> result = new ArrayList<>();
        while (in1.hasNext()) {
          Feature f1 = in1.next();

          for(Feature f2: in2) {
            Feature r = f1.intersection(f2, attrReserved);
            if (!r.isEmpty()) {
              result.add(new Tuple2<>(input._1, r));
            }
          }
        }
        return result.iterator();
      }
    });
    Layer olayer = new Layer(res.rdd());
    return new KeyIndexedLayer(olayer, this);
  }

  public void makeSureCached() {
    this.makeSureCached(StorageLevel.MEMORY_ONLY());
  }

  public void makeSureCached(StorageLevel level) {
    this.layer.makeSureCached(level);
  }

  public void release() {
    this.layer.release();
  }

}
