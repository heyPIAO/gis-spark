package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * 轨迹点：相较于普通的Point，多时间戳这个属性
 */
@Getter
@Setter
public class TrajectoryPoint extends Point {

    protected long timestamp;

    public TrajectoryPoint(String fid, org.locationtech.jts.geom.Point geometry, Map<String, Object> attributes, long timestamp) {
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

    @Override
    protected Map<String, Object> getGeometryMap() {
        Map<String, Object> geometryMap = super.getGeometryMap();
        geometryMap.put("timestamp", this.timestamp);
        geometryMap.put("type", "TPoint"); // TPoint: Timed-Point
        return geometryMap;
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
