package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.PointFeature;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;


/**
 * @author Hu
 * @date 2019/9/19
 *
 **/
public class PointLayer extends Layer<String, PointFeature> {

  public PointLayer() {}

  public PointLayer(RDD<Tuple2<String, PointFeature>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(PointFeature.class));
  }

  private PointLayer(RDD<Tuple2<String, PointFeature>> rdd, ClassTag<String> kClassTag, ClassTag<PointFeature> pointClassTag) {
    this(rdd, kClassTag, pointClassTag, false);
  }

  private PointLayer(RDD<Tuple2<String, PointFeature>> rdd, ClassTag<String> kClassTag, ClassTag<PointFeature> pointClassTag, boolean hasIndexed) {
    super(rdd, kClassTag, pointClassTag, hasIndexed);
  }

  @Override
  public PointLayer initialize(RDD<Tuple2<String, PointFeature>> rdd) {
    return new PointLayer(rdd);
  }


}
