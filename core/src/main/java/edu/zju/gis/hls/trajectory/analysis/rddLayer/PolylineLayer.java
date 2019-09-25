package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.PolylineFeature;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public class PolylineLayer extends Layer<String, PolylineFeature> {

  public PolylineLayer(RDD<Tuple2<String, PolylineFeature>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(PolylineFeature.class));
  }

  private PolylineLayer(RDD<Tuple2<String, PolylineFeature>> rdd, ClassTag<String> kClassTag, ClassTag<PolylineFeature> polylineClassTag) {
    this(rdd, kClassTag, polylineClassTag, false);
  }

  private PolylineLayer(RDD<Tuple2<String, PolylineFeature>> rdd, ClassTag<String> kClassTag, ClassTag<PolylineFeature> polylineClassTag, boolean hasIndexed) {
    super(rdd, kClassTag, polylineClassTag, hasIndexed);
  }

}
