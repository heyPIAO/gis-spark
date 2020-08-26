package edu.zju.gis.hls.trajectory.analysis.index.partitioner;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.Utils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.precision.EnhancedPrecisionOp;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 自定义空间数据分区器
 * @author Hu
 * @date 2020/8/24
 * 思路：
 * （1）利用 PreProcess 函数将 PairRDD 的 key 转为分区索引的 key
 * （2）利用分区索引的 key 将原始 PairRDD 进行分区，分区原理本质上和 HashPartitioner 是一样的
 **/
@Getter
@Setter
@NoArgsConstructor
public abstract class DistributeSpatialPartitioner extends Partitioner
  implements FlatMapFunction<Tuple2<String, Feature>, Tuple2<String, Feature>>, Serializable {

  protected Map<String, KeyRangeFeature> keyRanges;

  protected int partitionNum;

  public DistributeSpatialPartitioner(int partitionNum) {
    super();
    this.partitionNum = partitionNum;
    this.keyRanges = new HashMap<>();
  }

  /**
   * 获取与Geometry相交的分区 grid 所对应的 key
   * 一个 geometry 可能会覆盖多个分区 Grid
   * @param geometry
   * @return
   */
  public abstract List<String> getKey(Geometry geometry);

  /**
   * 根据 Feature 获取对应的 key
   * @param feature
   * @return
   */
  public List<Tuple2<String, Feature>> getKey(Feature feature) {
    return this.getKey(feature.getGeometry()).stream().map(x->new Tuple2<>(x, feature)).collect(Collectors.toList());
  }

  /**
   * 获取与Geometry相交的分区 grid 的 Partition Range Feature
   * 一个 geometry 可能会覆盖多个分区 Grid
   * @param geometry
   * @return
   */
  public List<KeyRangeFeature> getKeyRangeFeatures(Geometry geometry) {
    return this.getKey(geometry).stream().map(x->this.getKeyRangeFeature(x)).collect(Collectors.toList());
  }

  public List<KeyRangeFeature> getKeyRangeFeatures(Feature feature) {
    return this.getKeyRangeFeatures(feature.getGeometry());
  }

  @Override
  public int numPartitions() {
    return this.partitionNum;
  }

  /**
   * 获取指定分区 key 的区域
   * @param key
   * @return
   */
  public abstract KeyRangeFeature getKeyRangeFeature(String key);

  /**
   * 根据分区key获取分区序号，与 HashPartition 原理上一致
   * @param key
   * @return
   */
  @Override
  public int getPartition(Object key) {
    return Utils.nonNegativeMod(((String) key).hashCode(), partitionNum);
  }

  @Override
  public Iterator<Tuple2<String, Feature>> call(Tuple2<String, Feature> in) throws Exception {
    List<Tuple2<String, Feature>> result = new ArrayList<>();
    List<KeyRangeFeature> keyRangeFeatures = this.getKeyRangeFeatures(in._2);
    // TODO 作为参数传入
    boolean isClip = true;
    for (KeyRangeFeature keyRangeFeature : keyRangeFeatures) {
      if (isClip) {
        Polygon p = keyRangeFeature.getGeometry();
        Geometry geom = in._2.getGeometry();
        Geometry finalGeom;
        if (p.contains(geom)) {
          finalGeom = geom;
        } else {
          try {
            finalGeom = EnhancedPrecisionOp.intersection(p, geom);
          } catch (TopologyException e) {
            // 对于自相交图形，计算 intersection 会产生拓扑错误
            // TODO 用 buffer 方法解决会导致一部分的图斑缺失，待支持MultiPolygon//MultiLineString的时候需要改成将图斑自动切分的方法
            // TODO https://stackoverflow.com/questions/31473553/is-there-a-way-to-convert-a-self-intersecting-polygon-to-a-multipolygon-in-jts
            p = (Polygon) p.buffer(0);
            geom = geom.buffer(0);
            finalGeom = EnhancedPrecisionOp.intersection(p, geom);
          }
        }
        if (!finalGeom.isEmpty()) {
          Feature f = in._2.getSelfCopyObject();
          f.setGeometry(finalGeom);
          result.add(new Tuple2<>(keyRangeFeature.getFid(), f));
        }
      } else {
        result.add(new Tuple2<>(keyRangeFeature.getFid(), in._2));
      }
    }
    return result.iterator();
  }

  /**
   * 分区前的预处理函数
   * Hip：这一步已经将 PairRDD 的 key 转为分区 gridID 的 key
   * 处理一个 Geometry 可能会覆盖多个 Grid 的情况
   * @return
   */
//  public static <K, V extends Feature, P extends DistributeSpatialPartitioner> FlatMapFunction getPreProcessFunction(P partitioner, boolean isClip) {
//    return new FlatMapFunction<Tuple2<K, V>, Tuple2<String, V>>() {
//      @Override
//      public Iterator<Tuple2<String, V>> call(Tuple2<K, V> in) throws Exception {
//        List<Tuple2<String, V>> result = new ArrayList<>();
//        List<KeyRangeFeature> keyRangeFeatures = partitioner.getKeyRangeFeatures(in._2);
//        for (KeyRangeFeature keyRangeFeature : keyRangeFeatures) {
//          if (isClip) {
//            Polygon p = keyRangeFeature.getGeometry();
//            Geometry geom = in._2.getGeometry();
//            Geometry finalGeom;
//            if (p.contains(geom)) {
//              finalGeom = geom;
//            } else {
//              try {
//                finalGeom = EnhancedPrecisionOp.intersection(p, geom);
//              } catch (TopologyException e) {
//                // 对于自相交图形，计算 intersection 会产生拓扑错误
//                // TODO 用 buffer 方法解决会导致一部分的图斑缺失，待支持MultiPolygon//MultiLineString的时候需要改成将图斑自动切分的方法
//                // TODO https://stackoverflow.com/questions/31473553/is-there-a-way-to-convert-a-self-intersecting-polygon-to-a-multipolygon-in-jts
//                p = (Polygon) p.buffer(0);
//                geom = geom.buffer(0);
//                finalGeom = EnhancedPrecisionOp.intersection(p, geom);
//              }
//            }
//            if (!finalGeom.isEmpty()) {
//              V f = (V) in._2.getSelfCopyObject();
//              f.setGeometry(finalGeom);
//              result.add(new Tuple2<>(keyRangeFeature.getFid(), f));
//            }
//          } else {
//            result.add(new Tuple2<>(keyRangeFeature.getFid(), in._2));
//          }
//        }
//        return result.iterator();
//      }
//    };
//  }
}
