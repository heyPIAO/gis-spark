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
 **/
public abstract class BinaryOperatorImpl implements BinaryOperator {

  private SparkSession ss;

  private boolean attrReserve;

  public BinaryOperatorImpl(SparkSession ss, boolean attrReserve) {
    this.ss = ss;
    this.attrReserve = attrReserve;
  }

  public BinaryOperatorImpl(SparkSession ss) {
    this(ss, false);
  }

  @Override
  public Layer run(Feature f, Layer layer) {
    Layer l = this.run(f.getGeometry(), layer);
    if (attrReserve) {
      return unionAttrs(f, l);
    }
    return l;
  }

  @Override
  public Layer run(Feature f, IndexedLayer layer) {
    Layer l = this.run(f.getGeometry(), layer);
    if (attrReserve) {
      return unionAttrs(f, l);
    }
    return l;
  }

  private Layer unionAttrs(Feature f, Layer layer) {

    Function unionAttrFunction = new Function<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
      @Override
      public Tuple2<String, Feature> call(Tuple2<String, Feature> v1) throws Exception {
        v1._2.addAttributes(f.getAttributes(), f.getFid());
        return v1;
      }
    };

    return layer.mapToLayer(unionAttrFunction);
  }

  @Override
  public Layer run(Geometry f, IndexedLayer layer) {
    return this.run(f, layer.query(f).toLayer());
  }

  @Override
  public Layer run(IndexedLayer layer1, IndexedLayer layer2) {
    throw new GISSparkException("Under developing");
  }

}
