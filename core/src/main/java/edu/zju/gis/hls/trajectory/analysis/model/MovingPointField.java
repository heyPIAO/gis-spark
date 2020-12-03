package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.SHAPE_FIELD;

public class MovingPointField extends Field {
    public MovingPointField() {
        super(SHAPE_FIELD.name(), TemporalLineString.class.getName(), FieldType.SHAPE_FIELD);
    }
}
