package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.model.TimedPoint;
import edu.zju.gis.hls.trajectory.analysis.model.MovingPoint;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.datastore.exception.WriterException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.locationtech.jts.geom.Coordinate;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.*;

/**
 * @author Hu
 * @date 2019/9/19
 **/
@Slf4j
public class TrajectoryPointLayer extends Layer<String, TimedPoint> {

  public TrajectoryPointLayer(RDD<Tuple2<String, TimedPoint>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(TimedPoint.class));
  }

  private TrajectoryPointLayer(RDD<Tuple2<String, TimedPoint>> rdd, ClassTag<String> kClassTag, ClassTag<TimedPoint> trajectoryPointClassTag) {
    super(rdd, kClassTag, trajectoryPointClassTag);
  }


    /**
     * 将轨迹点根据指定 id 构成轨迹线
     * 默认保留最后一个点属性作为轨迹线的属性
     * @param attr 指定聚合字段，用于将点聚合为线
     * @return
     * Hint: 默认attr为静态属性
     */
  public TrajectoryPolylineLayer convertToPolylineLayer (String attr, String... staticAttr) {

    if (this.findAttribute(attr) == null) {
      throw new WriterException(String.format("attribute named after %s, not exists", attr));
    }

    JavaRDD<Tuple2<String, TimedPoint>> t = this.rdd().toJavaRDD();
    JavaPairRDD<String, Iterable<Tuple2<String, TimedPoint>>> tg = t.groupBy(new Function<Tuple2<String, TimedPoint>, String>() {
      @Override
      public String call(Tuple2<String, TimedPoint> in) throws Exception {
        return (String) in._2.getAttribute(attr);
      }
    });

    JavaRDD<Tuple2<String, MovingPoint>> tl = tg.map(new Function<Tuple2<String, Iterable<Tuple2<String, TimedPoint>>>, Tuple2<String, MovingPoint>>() {
      @Override
      public Tuple2<String, MovingPoint> call(Tuple2<String, Iterable<Tuple2<String, TimedPoint>>> in) throws Exception {
        List<TimedPoint> ps = new ArrayList<>();
        for (Tuple2<String, TimedPoint> tp: in._2) {
          ps.add(tp._2);
        }
        // 根据时间戳排序
        ps.sort((o1, o2) -> {
          if (o1.getTimestamp() < o2.getTimestamp())
            return -1;
          return 1;
        });

        Coordinate[] coordinates = new Coordinate[ps.size()];
        for (int i=0; i<ps.size(); i++){
          coordinates[i] = ps.get(i).getGeometry().getCoordinate();
        }
        TemporalLineString pl;
        if (coordinates.length < 2) {
          log.warn(String.format("Only One point for %s, abort", ps.get(0).toString()));
          return new Tuple2<>("EMPTY", null);
        }

        // 获取时间戳序列
        long[] instants = new long[ps.size()];
        for (int i=0; i<instants.length; i++) {
            instants[i] = ps.get(i).getTimestamp();
        }

        pl = Term.CUSTOM_GEOMETRY_FACTORY.createTemporalLineString(coordinates, instants);

        // 构建轨迹静态属性,以轨迹最后一个点的静态属性为准
        TimedPoint plast = ps.get(ps.size()-1);
        LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();
        Field fattr = plast.getField(attr);
          attributes.put(fattr, plast.getAttribute(fattr));
        for (int i=0; i<staticAttr.length; i++) {
            Field fsa = plast.getField(staticAttr[i]);
            attributes.put(fsa, plast.getAttribute(fsa));
        }

        // 构建轨迹动态属性
        LinkedHashMap<Field, Object[]> dynamicAttributes = new LinkedHashMap<>();
        List<Field> dynamicFields = new ArrayList<>();
        for (Field f: plast.getAttributes().keySet()) {
            if (!attributes.containsKey(f)) dynamicFields.add(f);
        }

        for (Field f: dynamicFields) {
            Object[] os = new Object[ps.size()];
            for (int i=0; i<os.length; i++) {
                os[i] = ps.get(i).getAttribute(f);
            }
            dynamicAttributes.put(f, os);
        }

        // 初始化移动点，即轨迹
        MovingPoint l = new MovingPoint(String.valueOf(in._1), pl, attributes, dynamicAttributes);

        l.setFid(UUID.randomUUID().toString());
        return new Tuple2<>(l.getFid(), l);
      }
    });

    tl = tl.filter(x->!x._1.equals("EMPTY"));
    TrajectoryPolylineLayer result = new TrajectoryPolylineLayer(tl.rdd());
    LayerMetadata meta = new LayerMetadata(this.metadata);
    Field shapeField = meta.getShapeField();
    shapeField.setType(TemporalLineString.class);
    result.setMetadata(meta);

    return result;
  }

}
