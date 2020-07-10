package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author Hu
 * @date 2020/7/10
 * 空间擦除操作
 * TODO 待测
 **/
public class EraseOperator extends BinaryOperatorImpl {

  public EraseOperator(SparkSession ss, boolean attrReserve) {
    super(ss, attrReserve);
  }

  public EraseOperator(SparkSession ss) {
    super(ss);
  }

  private Layer erase(Geometry f, Layer layer) {
    Function eraseFunction = new Function<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
      @Override
      public Tuple2<String, Feature> call(Tuple2<String, Feature> v1) throws Exception {

        Geometry g = v1._2.getGeometry();

        if (g.isEmpty() || !g.intersects(f)) return new Tuple2<>(v1._1, Feature.empty());

        Geometry r = g.difference(f);
        if (r.isEmpty()) return new Tuple2<>(v1._1, Feature.empty());

        return new Tuple2<>(v1._1, Feature.buildFeature(v1._2.getFid(), r, v1._2.getAttributes()));
      }
    };

    Function filterFunction = new Function<Tuple2<String, Feature>, Boolean> () {
      @Override
      public Boolean call(Tuple2<String, Feature> v) throws Exception {
        return !v._2.isEmpty();
      }
    };
    return layer.mapToLayer(eraseFunction).filterToLayer(filterFunction);
  }

  @Override
  public Layer run(Geometry f, Layer layer) {
    return this.erase(f, layer);
  }

}
