package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;

/**
 * 轨迹点：相较于普通的Point，多时间戳这个属性
 */
@Getter
@Setter
public class TimedPoint extends Feature<TemporalPoint> {

    public TimedPoint(String fid, TemporalPoint geometry, LinkedHashMap<Field, Object> attributes) {
        super(fid, geometry, attributes);
    }

    public TimedPoint(TimedPoint p) {
        super(p);
    }

    public long getTimestamp() {
        return this.geometry.getInstant();
    }

}
