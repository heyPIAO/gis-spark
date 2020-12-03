package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Polyline;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public class PolylineLayer extends Layer<String, Polyline> {

  public PolylineLayer(RDD<Tuple2<String, Polyline>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(Polyline.class));
  }

  private PolylineLayer(RDD<Tuple2<String, Polyline>> rdd, ClassTag<String> kClassTag, ClassTag<Polyline> polylineClassTag) {
    super(rdd, kClassTag, polylineClassTag);
  }

}
