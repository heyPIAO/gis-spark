package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Point;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;


/**
 * @author Hu
 * @date 2019/9/19
 *
 **/
public class PointLayer extends Layer<String, Point> {

  public PointLayer() {}

  public PointLayer(RDD<Tuple2<String, Point>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(Point.class));
  }

  private PointLayer(RDD<Tuple2<String, Point>> rdd, ClassTag<String> kClassTag, ClassTag<Point> pointClassTag) {
    super(rdd, kClassTag, pointClassTag);
  }

}
