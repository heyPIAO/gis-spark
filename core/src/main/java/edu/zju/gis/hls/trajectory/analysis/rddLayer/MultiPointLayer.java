package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.MultiPoint;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;


/**
 * @author Hu
 * @date 2019/9/19
 *
 **/
public class MultiPointLayer extends Layer<String, MultiPoint> {

  public MultiPointLayer(RDD<Tuple2<String, MultiPoint>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(MultiPoint.class));
  }

  private MultiPointLayer(RDD<Tuple2<String, MultiPoint>> rdd, ClassTag<String> kClassTag, ClassTag<MultiPoint> multiPointClassTag) {
    super(rdd, kClassTag, multiPointClassTag);
  }

}
