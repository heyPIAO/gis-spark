package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.PolygonFeature;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * @author Hu
 * @date 2019/9/20
 **/
public class PolygonLayer extends Layer<String, PolygonFeature> {

  public PolygonLayer() {}

  public PolygonLayer(RDD<Tuple2<String, PolygonFeature>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(PolygonFeature.class));
  }

  private PolygonLayer(RDD<Tuple2<String, PolygonFeature>> rdd, ClassTag<String> kClassTag, ClassTag<PolygonFeature> polygonFeatureClassTag) {
    this(rdd, kClassTag, polygonFeatureClassTag, false);
  }


  public PolygonLayer(RDD<Tuple2<String, PolygonFeature>> rdd, ClassTag<String> stringClassTag, ClassTag<PolygonFeature> polygonFeatureClassTag, boolean hasIndexed) {
    super(rdd, stringClassTag, polygonFeatureClassTag, hasIndexed);
  }

  @Override
  public PolygonLayer initialize(RDD<Tuple2<String, PolygonFeature>> rdd) {
    return new PolygonLayer(rdd);
  }
}
