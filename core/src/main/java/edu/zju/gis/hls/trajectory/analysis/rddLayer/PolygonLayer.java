package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Polygon;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * @author Hu
 * @date 2019/9/20
 **/
public class PolygonLayer extends Layer<String, Polygon> {

  public PolygonLayer(RDD<Tuple2<String, Polygon>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(Polygon.class));
  }

  private PolygonLayer(RDD<Tuple2<String, Polygon>> rdd, ClassTag<String> kClassTag, ClassTag<Polygon> polygonFeatureClassTag) {
    super(rdd, kClassTag, polygonFeatureClassTag);
  }

}
