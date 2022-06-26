package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

public class GeometryLayer extends Layer<String, Feature> {
    public GeometryLayer(RDD<Tuple2<String, Feature>> rdd) {
        super(rdd);
    }
}
