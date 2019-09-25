package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.PointFeature;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;


/**
 * @author Hu
 * @date 2019/9/19
 *
 **/
public class PointLayer extends Layer<String, PointFeature> {

  public PointLayer(RDD<Tuple2<String, PointFeature>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(PointFeature.class));
  }

  private PointLayer(RDD<Tuple2<String, PointFeature>> rdd, ClassTag<String> kClassTag, ClassTag<PointFeature> pointClassTag) {
    this(rdd, kClassTag, pointClassTag, false);
  }

  private PointLayer(RDD<Tuple2<String, PointFeature>> rdd, ClassTag<String> kClassTag, ClassTag<PointFeature> pointClassTag, boolean hasIndexed) {
    super(rdd, kClassTag, pointClassTag, hasIndexed);
  }

  /**
   * 点偏移
   * @param deltaX
   * @param deltaY
   * @return
   */
  public PointLayer shift(double deltaX, double deltaY) {
    JavaRDD<Tuple2<String, PointFeature>> t = this.rdd().toJavaRDD();
    JavaRDD<Tuple2<String, PointFeature>> r = t.map(new Function<Tuple2<String, PointFeature>, Tuple2<String, PointFeature>>() {
      @Override
      public Tuple2<String, PointFeature> call(Tuple2<String, PointFeature> in) throws Exception {
        PointFeature pf = in._2.shift(deltaX, deltaY);
        return new Tuple2<>(pf.getFid(), pf);
      }
    });
    return new PointLayer(r.rdd());
  }

}
