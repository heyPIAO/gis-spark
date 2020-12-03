package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.MultiPolyline;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;


/**
 * @author Hu
 * @date 2019/9/19
 **/
public class MultiPolylineLayer extends Layer<String, MultiPolyline> {

  public MultiPolylineLayer(RDD<Tuple2<String, MultiPolyline>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(MultiPolyline.class));
  }

  private MultiPolylineLayer(RDD<Tuple2<String, MultiPolyline>> rdd, ClassTag<String> kClassTag, ClassTag<MultiPolyline> multiPolylineClassTag) {
    super(rdd, kClassTag, multiPolylineClassTag);
  }

}
