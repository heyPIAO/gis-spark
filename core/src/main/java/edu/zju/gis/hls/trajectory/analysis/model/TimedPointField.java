package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.SHAPE_FIELD;

public class TimedPointField extends Field {
    public TimedPointField() {
        super(SHAPE_FIELD.name(), TemporalPoint.class.getName(), FieldType.SHAPE_FIELD);
    }
}
