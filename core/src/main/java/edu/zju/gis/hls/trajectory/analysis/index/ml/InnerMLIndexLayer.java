package edu.zju.gis.hls.trajectory.analysis.index.ml;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.ml.model.NNModelIndex;
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
import scala.Serializable;
import scala.Tuple2;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;

/**
 * @author Hu
 * @date 2019/12/24
 * TODO 待测
 **/
@Slf4j
public class InnerMLIndexLayer<L extends Layer> extends PartitionIndexedLayer<L, KeyIndexedLayer<L>> implements Serializable {

  @Getter
  @Setter
  private JavaPairRDD<String, NNModelIndex> indexedPartition;

  public InnerMLIndexLayer() {
    this.indexType = IndexType.NN;
  }

  @Override
  public void makeSureCached() {
    this.layer.makeSureCached();
    this.indexedPartition.cache();
  }

  @Override
  public void unpersist() {
    this.layer.release();
    this.indexedPartition.unpersist();
  }

  /**
   * @param geometry
   * @return
   */
  @Override
  public KeyIndexedLayer<L> query(Geometry geometry) {
    List<String> partitionIds = this.layer.queryPartitionsKeys(geometry);
    JavaPairRDD<String, NNModelIndex> partitions = indexedPartition.filter(m->partitionIds.contains(m._1));
    JavaRDD<Tuple2<String, Feature>> t = partitions
      .flatMap(new FlatMapFunction<Tuple2<String, NNModelIndex>, Tuple2<String, Feature>>() {
        @Override
        public Iterator<Tuple2<String, Feature>> call(Tuple2<String, NNModelIndex> in)  {
          return (in._2.query(geometry)).iterator();
        }
      });

    try {
      Constructor con = this.layer.toLayer().getConstructor(RDD.class);
      L l = (L) con.newInstance(t.rdd());
      this.getLayer().setLayer(l);
      return this.getLayer();
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
      throw new GISSparkException("InnerMLIndexLayer query failed: " + e.getMessage());
    }
  }

}

