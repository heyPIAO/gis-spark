package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

/**
 * @author Hu
 * @date 2020/7/9
 * 空间相交操作
 * TODO 待测
 **/
public class ClipOperator extends BinaryOperatorImpl {

  public ClipOperator(SparkSession ss) {
    super(ss);
  }

  public Layer clip(Geometry f, Layer layer) {

    Function clipFunction = new Function<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
      @Override
      public Tuple2<String, Feature> call(Tuple2<String, Feature> v1) throws Exception {

        Geometry g = v1._2.getGeometry();

        if (g.isEmpty() || !g.intersects(f)) return new Tuple2<>(v1._1, Feature.empty());

        Geometry r = f.intersection(g);
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
    return layer.mapToLayer(clipFunction).filterToLayer(filterFunction);
  }

  @Override
  public Layer run(Geometry f, Layer layer) {
    if (!(f instanceof org.locationtech.jts.geom.Polygon || f instanceof org.locationtech.jts.geom.MultiPolygon)) {
      throw new GISSparkException("Unvalid operator for geometry type: " + f.toString());
    }
    return this.clip(f, layer);
  }

  @Override
  public Layer run(Geometry f, IndexedLayer layer) {
    return this.clip(f, layer.query(f).toLayer());
  }

  @Override
  public Layer run(IndexedLayer layer1, IndexedLayer layer2) {
    throw new GISSparkException("Under developing");
  }

}
