package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryPoint;
import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryPolyline;
import edu.zju.gis.hls.trajectory.datastore.exception.WriterException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.*;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public class TrajectoryPointLayer extends Layer<String, TrajectoryPoint> {

  private static final Logger logger = LoggerFactory.getLogger(TrajectoryPointLayer.class);

  public TrajectoryPointLayer(RDD<Tuple2<String, TrajectoryPoint>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(TrajectoryPoint.class));
  }

  private TrajectoryPointLayer(RDD<Tuple2<String, TrajectoryPoint>> rdd, ClassTag<String> kClassTag, ClassTag<TrajectoryPoint> trajectoryPointClassTag) {
    super(rdd, kClassTag, trajectoryPointClassTag);
  }

  /**
   * 将轨迹点根据指定 id 构成轨迹线
   * 默认保留最后一个点属性作为轨迹线的属性
   * @return
   */
  public TrajectoryPolylineLayer convertToPolylineLayer (String attr) {

    if (this.findAttribute(attr) == null) {
      throw new WriterException(String.format("attribute named after %s, not exists", attr));
    }

    JavaRDD<Tuple2<String, TrajectoryPoint>> t = this.rdd().toJavaRDD();
    JavaPairRDD<String, Iterable<Tuple2<String, TrajectoryPoint>>> tg = t.groupBy(new Function<Tuple2<String, TrajectoryPoint>, String>() {
      @Override
      public String call(Tuple2<String, TrajectoryPoint> in) throws Exception {
        return (String) in._2.getAttribute(attr);
      }
    });

    JavaRDD<Tuple2<String, TrajectoryPolyline>> tl = tg.map(new Function<Tuple2<String, Iterable<Tuple2<String, TrajectoryPoint>>>, Tuple2<String, TrajectoryPolyline>>() {
      @Override
      public Tuple2<String, TrajectoryPolyline> call(Tuple2<String, Iterable<Tuple2<String, TrajectoryPoint>>> in) throws Exception {
        List<TrajectoryPoint> ps = new ArrayList<>();
        for (Tuple2<String, TrajectoryPoint> tp: in._2) {
          ps.add(tp._2);
        }
        // 根据时间戳排序
        ps.sort(new Comparator<TrajectoryPoint>() {
          @Override
          public int compare(TrajectoryPoint o1, TrajectoryPoint o2) {
            if (o1.getTimestamp() < o2.getTimestamp())
              return -1;
            return 1;
          }
        });

        long startTime = ps.get(0).getTimestamp();
        long endTime = ps.get(ps.size()-1).getTimestamp();
        Coordinate[] coordinates = new Coordinate[ps.size()];
        for (int i=0; i<ps.size(); i++){
          coordinates[i] = ps.get(i).getGeometry().getCoordinate();
        }
        LineString pl;
        if (coordinates.length < 2) {
          logger.warn(String.format("Only One point for %s, abort", ps.get(0).toString()));
          return new Tuple2<>("EMPTY", null);
        }

        pl = new GeometryFactory().createLineString(coordinates);

        TrajectoryPoint plast = ps.get(ps.size()-1);
        Map<Field, Object> attributes = plast.getAttributes();

        TrajectoryPolyline l = new TrajectoryPolyline(String.valueOf(in._1), pl, attributes, startTime, endTime);
        l.setFid(UUID.randomUUID().toString());
        return new Tuple2<>(l.getFid(), l);
      }
    });

    tl = tl.filter(x->!x._1.equals("EMPTY"));
    TrajectoryPolylineLayer result = new TrajectoryPolylineLayer(tl.rdd());
    result.setMetadata(this.metadata);

    return result;
  }

}
