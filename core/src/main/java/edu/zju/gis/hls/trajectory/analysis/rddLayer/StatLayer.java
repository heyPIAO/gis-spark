package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Point;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.LinkedHashMap;

/**
 * @author Hu
 * @date 2020/9/10
 * 非空间图层，其 Geometry 为 Empty
 * TODO 目前无法从数据读取接口获取，只是作为分析计算结果的承接图层
 **/
public class StatLayer extends PointLayer {

  public StatLayer(JavaRDD<Tuple2<String, LinkedHashMap<Field, Object>>> rdd) {
    this(rdd.map(new Function<Tuple2<String, LinkedHashMap<Field, Object>>, Tuple2<String, Point>>() {
      @Override
      public Tuple2<String, Point> call(Tuple2<String, LinkedHashMap<Field, Object>> v1) throws Exception {
        Point p = Feature.empty();
        p.addAttributes(v1._2);
        p.setFid(v1._1);
        return new Tuple2<>(v1._1, p);
      }
    }).rdd());
  }

  public StatLayer(RDD<Tuple2<String, Point>> rdd){
    super(rdd);
  }

}
