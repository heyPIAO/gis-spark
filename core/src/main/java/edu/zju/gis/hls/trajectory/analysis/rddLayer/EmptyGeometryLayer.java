package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.EmptyGeometry;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * 空图层
 * @author Keran Sun (katus)
 * @version 1.0, 2020-08-13
 */
public class EmptyGeometryLayer extends Layer<String, EmptyGeometry> {
  public EmptyGeometryLayer() {
    this(null);
  }

  public EmptyGeometryLayer(RDD<Tuple2<String, EmptyGeometry>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(EmptyGeometry.class));
  }

  private EmptyGeometryLayer(RDD<Tuple2<String, EmptyGeometry>> rdd, ClassTag<String> kClassTag, ClassTag<EmptyGeometry> emptyGeometryClassTag) {
    super(rdd, kClassTag, emptyGeometryClassTag);
  }
}
