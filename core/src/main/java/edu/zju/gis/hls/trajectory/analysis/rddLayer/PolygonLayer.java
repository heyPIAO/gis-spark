package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Polygon;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

  /**
   * Geotools::JTS::makeValid
   * @param removeHoles
   * @return
   */
  public PolygonLayer makeValid(boolean removeHoles) {
    FlatMapFunction<Tuple2<String, Polygon>, Tuple2<String, Polygon>> f =
      new FlatMapFunction<Tuple2<String, Polygon>, Tuple2<String, Polygon>> () {
      @Override
      public Iterator<Tuple2<String, Polygon>> call(Tuple2<String, Polygon> t) throws Exception {
        List<Tuple2<String, Polygon>> result = new ArrayList<>();
        List<Polygon> ps = t._2.makeValid(removeHoles);
        for (Polygon p: ps) {
          result.add(new Tuple2<>(t._1, p));
        }
        return result.iterator();
      }
    };
    return (PolygonLayer)this.flatMapToLayer(f);
  }

}
