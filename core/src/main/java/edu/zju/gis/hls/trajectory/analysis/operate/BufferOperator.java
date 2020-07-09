package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.MultiPolygon;
import edu.zju.gis.hls.trajectory.analysis.model.Polygon;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PolygonLayer;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * @author Hu
 * @date 2020/7/8
 * 缓冲区操作
 **/
@Getter
@Setter
public class BufferOperator extends OperatorImpl {

  private double radius;

  public BufferOperator(SparkSession ss, double radius) {
    super(ss);
    this.radius = radius;
  }

  @Override
  public Layer operate(Layer layer) {
    JavaPairRDD<String, Feature> buffered = layer.mapToPair(new PairFunction() {
      @Override
      public Tuple2 call(Object o) throws Exception {
        Tuple2<String, Feature> f = (Tuple2<String, Feature>)o;
        return new Tuple2<>(f._1, f._2.buffer(radius));
      }
    });

    if (layer.isSimpleLayer()) {
      return new PolygonLayer(buffered.mapToPair(x ->
        new Tuple2<String, Polygon>(x._1, (Polygon)x._2)).rdd());
    } else {
      return new MultiPolygonLayer(buffered.mapToPair(x ->
        new Tuple2<String, MultiPolygon>(x._1, (MultiPolygon)x._2)).rdd());
    }
  }

}
