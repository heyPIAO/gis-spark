package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.MultiPolygon;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.List;


/**
 * @author Hu
 * @date 2019/9/19
 *
 **/
public class MultiPolygonLayer extends Layer<String, MultiPolygon> {

  public MultiPolygonLayer(JavaSparkContext jsc, List<Tuple2<String, MultiPolygon>> features) {
    super(jsc, features);
  }

  public MultiPolygonLayer(RDD<Tuple2<String, MultiPolygon>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(MultiPolygon.class));
  }

  private MultiPolygonLayer(RDD<Tuple2<String, MultiPolygon>> rdd, ClassTag<String> kClassTag, ClassTag<MultiPolygon> multiPolygonClassTag) {
    super(rdd, kClassTag, multiPolygonClassTag);
  }

}
