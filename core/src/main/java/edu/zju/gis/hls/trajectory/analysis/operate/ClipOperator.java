package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.*;

/**
 * @author Hu
 * @date 2020/7/9
 * 空间裁切操作
 * TODO 整理重复代码
 * TODO 待测
 **/
public class ClipOperator extends BinaryOperatorImpl {

  public ClipOperator(SparkSession ss) {
    super(ss);
  }

  public ClipOperator(SparkSession ss, boolean attrReserve) {
    super(ss, attrReserve);
  }

  @Override
  public Layer run(List<Feature> features, Layer layer) {
    return layer.flatMapToLayer(this.clipFunction(features.toArray(new Feature[]{}), attrReserve)).filterEmpty();
  }

  private FlatMapFunction clipFunction(Feature[] features, Boolean attrReserved) {
    return new FlatMapFunction<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
      @Override
      public Iterator<Tuple2<String, Feature>> call(Tuple2<String, Feature> v1) throws Exception {

        List<Tuple2<String, Feature>> result = new ArrayList<>();
        Geometry g = v1._2.getGeometry();

        for (int i=0; i<features.length; i++) {
          Feature f = features[i];
          if (g.isEmpty() || !g.intersects(f.getGeometry())) {
            result.add(new Tuple2<>(String.format("%s_%s", v1._1, f.getFid()), Feature.empty()));
            continue;
          }
          Geometry r = g.intersection(f.getGeometry());
          if (r.isEmpty()) {
            result.add(new Tuple2<>(String.format("%s_%s", v1._1, f.getFid()), Feature.empty()));
          } else {
            Feature of = Feature.buildFeature(v1._2.getFid(), r, v1._2.getAttributes());
            if (attrReserved) {
              of.addAttributes(f.getAttributes(), f.getFid());
            }
            result.add(new Tuple2<>(String.format("%s_%s", v1._1, f.getFid()), of));
          }
        }
        return result.iterator();
      }
    };
  }

}
