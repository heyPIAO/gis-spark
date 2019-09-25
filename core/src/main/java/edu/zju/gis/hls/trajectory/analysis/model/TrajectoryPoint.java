package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.PointLayer;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

import java.util.Map;

@Getter
@Setter
public class TrajectoryPoint extends PointFeature {

    protected long timestamp;

    public TrajectoryPoint(String fid, Point geometry, Map<String, Object> attributes, long timestamp) {
        super(fid, geometry, attributes);
        this.timestamp = timestamp;
    }

    public TrajectoryPoint(TrajectoryPoint p){
        super(p);
        this.timestamp = p.getTimestamp();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%d \t", timestamp));
        for(String k: attributes.keySet()){
            sb.append(String.valueOf(attributes.get(k)) + "\t");
        }
        sb.append(geometry.toString());
        return sb.toString();
    }


    public String toStringXY() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%d \t", timestamp));
        for(String k: attributes.keySet()){
            sb.append(String.valueOf(attributes.get(k)) + "\t");
        }
        sb.append(String.format("%.12f \t %.12f", this.geometry.getX(), this.geometry.getY()));
        return sb.toString();
    }

}
