package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryPolyline;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public class TrajectoryPolylineLayer extends Layer<String, TrajectoryPolyline> {

  public TrajectoryPolylineLayer(RDD<Tuple2<String, TrajectoryPolyline>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(TrajectoryPolyline.class));
  }

  private TrajectoryPolylineLayer(RDD<Tuple2<String, TrajectoryPolyline>> rdd, ClassTag<String> kClassTag, ClassTag<TrajectoryPolyline> trajectoryPolylineClassTag) {
    this(rdd, kClassTag, trajectoryPolylineClassTag, false);
  }

  public TrajectoryPolylineLayer(RDD<Tuple2<String, TrajectoryPolyline>> rdd, ClassTag<String> stringClassTag, ClassTag<TrajectoryPolyline> trajectoryPointClassTag, boolean hasIndexed) {
    super(rdd, stringClassTag, trajectoryPointClassTag, hasIndexed);
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
}
