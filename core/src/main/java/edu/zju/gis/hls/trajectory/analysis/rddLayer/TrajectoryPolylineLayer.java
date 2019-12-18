package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryOD;
import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryPolyline;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.*;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public class TrajectoryPolylineLayer extends Layer<String, TrajectoryPolyline> {

  public TrajectoryPolylineLayer() {}

  public TrajectoryPolylineLayer(RDD<Tuple2<String, TrajectoryPolyline>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(TrajectoryPolyline.class));
  }

  private TrajectoryPolylineLayer(RDD<Tuple2<String, TrajectoryPolyline>> rdd, ClassTag<String> kClassTag, ClassTag<TrajectoryPolyline> trajectoryPolylineClassTag) {
    super(rdd, kClassTag, trajectoryPolylineClassTag);
  }

  @Override
  public void setAttributes(Map<String, String> attributes) {
    super.setAttributes(attributes);
    this.attributes.put("starttime", "STARTTIME");
    this.attributes.put("endtime", "ENDTIME");
  }

  @Override
  public void setAttributeTypes(Map<String, String> attributeTypes) {
    super.setAttributeTypes(attributeTypes);
    this.attributeTypes.put("starttime", long.class.getName());
    this.attributeTypes.put("endtime", long.class.getName());
  }

  public TrajectoryODLayer extractOD() {
    JavaRDD<Tuple2<String, TrajectoryPolyline>> t = this.rdd().toJavaRDD();
    JavaRDD<Tuple2<String, TrajectoryOD>> tOD = t.map(new Function<Tuple2<String, TrajectoryPolyline>, Tuple2<String, TrajectoryOD>>() {
      @Override
      public Tuple2<String, TrajectoryOD> call(Tuple2<String, TrajectoryPolyline> in) throws Exception {
        TrajectoryOD tpOD = in._2.extractOD();
        if (tpOD == null) {
          return new Tuple2<>("EMPTY", null);
        }
        return new Tuple2<>(in._1, tpOD);
      }
    });
    tOD = tOD.filter(f -> !f._1.equals("EMPTY"));
    TrajectoryODLayer result = new TrajectoryODLayer(tOD.rdd());
    LayerMetadata lm = new LayerMetadata(metadata);
    lm.setLayerId("OD_" + metadata.getLayerId());
    lm.setLayerName("OD_" + metadata.getLayerName());
    // TODO 继承了原始轨迹线图层的四至，但并不一定准确
    result.setMetadata(this.metadata);
    return result;
  }

}
