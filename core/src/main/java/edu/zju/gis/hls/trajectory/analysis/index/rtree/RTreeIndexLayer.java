package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PartitionIndexedLayer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Hu
 * @date 2019/12/24
 **/
@Slf4j
public class RTreeIndexLayer<L extends Layer> extends PartitionIndexedLayer<L, KeyIndexedLayer<L>> {

  @Getter
  @Setter
  private JavaPairRDD<String, RTree> indexedPartition;

  public RTreeIndexLayer() {
    this.indexType = IndexType.RTREE;
  }

  @Override
  public RTreeIndexLayer<L> query(Geometry geometry) {
    List<String> partitionIds = this.layer.queryPartitionsIds(geometry);
    JavaRDD<Tuple2<String, Feature>> t = indexedPartition.filter(m->partitionIds.contains(m._1)).flatMap(new FlatMapFunction<Tuple2<String, RTree>, Tuple2<String, Feature>>() {
      @Override
      public Iterator<Tuple2<String, Feature>> call(Tuple2<String, RTree> in) throws Exception {
        List<Feature> l = in._2.query(geometry.getEnvelopeInternal());
        List<Tuple2<String, Feature>> result = new ArrayList<>();
        for (Feature f: l) {
          result.add(new Tuple2<>(in._1, f));
        }
        return result.iterator();
      }
    });

    try {
      Constructor con = this.layer.getLayer().getConstructor(RDD.class);
      L l = (L) con.newInstance(t.rdd());
      this.layer.setLayer(l);
      return this;
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
    }
    return null;
  }

}
