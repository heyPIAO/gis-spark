package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.TrajectoryOD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

/**
 * @author Hu
 * @date 2019/11/8
 **/
public class TrajectoryODLayer extends Layer<String, TrajectoryOD> {

  public TrajectoryODLayer() {}

  public TrajectoryODLayer(RDD<Tuple2<String, TrajectoryOD>> rdd){
    this(rdd, scala.reflect.ClassTag$.MODULE$.apply(String.class), scala.reflect.ClassTag$.MODULE$.apply(TrajectoryOD.class));
  }

  private TrajectoryODLayer(RDD<Tuple2<String, TrajectoryOD>> rdd, ClassTag<String> kClassTag, ClassTag<TrajectoryOD> trajectoryODClassTag) {
    super(rdd, kClassTag, trajectoryODClassTag);
  }

}
