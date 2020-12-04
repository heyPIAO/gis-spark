package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PartitionIndexedLayer;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
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
 * TODO 待测
  **/
@Slf4j
public class InnerRTreeIndexLayer<L extends Layer> extends PartitionIndexedLayer<L, KeyIndexedLayer<L>> {

  @Getter
  @Setter
  private JavaPairRDD<String, RTree> indexedPartition;

  public InnerRTreeIndexLayer() {
    this.indexType = IndexType.RTREE;
  }

  @Override
  public InnerRTreeIndexLayer<L> query(Geometry geometry) {
    List<String> partitionIds = this.layer.queryPartitionsKeys(geometry);
    JavaRDD<Tuple2<String, Feature>> t = indexedPartition.filter(m->partitionIds.contains(m._1)).flatMap(new FlatMapFunction<Tuple2<String, RTree>, Tuple2<String, Feature>>() {
      @Override
      public Iterator<Tuple2<String, Feature>> call(Tuple2<String, RTree> in) throws Exception {
        return (in._2.query(geometry)).iterator();
      }
    });

    // TODO 这步可能会报 NoSuchMethod 的错
    try {
      Constructor con = this.layer.toLayer().getConstructor(RDD.class);
      L l = (L) con.newInstance(t.rdd());
      this.getLayer().setLayer(l);
      return this;
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
      throw new GISSparkException("InnerRTreeIndexLayer query failed: " + e.getMessage());
    }
  }

}
