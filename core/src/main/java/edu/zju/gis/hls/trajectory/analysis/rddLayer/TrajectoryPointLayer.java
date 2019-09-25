package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryPoint;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public class TrajectoryPointLayer extends Layer<String, TrajectoryPoint> {

  public TrajectoryPointLayer(RDD<Tuple2<String, TrajectoryPoint>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(TrajectoryPoint.class));
  }

  private TrajectoryPointLayer(RDD<Tuple2<String, TrajectoryPoint>> rdd, ClassTag<String> kClassTag, ClassTag<TrajectoryPoint> trajectoryPointClassTag) {
    this(rdd, kClassTag, trajectoryPointClassTag, false);
  }

  public TrajectoryPointLayer(RDD<Tuple2<String, TrajectoryPoint>> rdd, ClassTag<String> stringClassTag, ClassTag<TrajectoryPoint> trajectoryPointClassTag, boolean hasIndexed) {
    super(rdd, stringClassTag, trajectoryPointClassTag, hasIndexed);
  }

  @Override
  public void setAttributes(Map<String, String> attributes) {
    super.setAttributes(attributes);
    this.attributes.put("timestamp", "TIMESTAMP");
  }

  @Override
  public void setAttributeTypes(Map<String, String> attributeTypes) {
    super.setAttributeTypes(attributeTypes);
    this.attributeTypes.put("timestamp", long.class.getName());
  }
}
