package edu.zju.gis.hls.trajectory.analysis.proto;

import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.TWKTWriter;
import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

@Getter
@Setter
public class TemporalPoint extends Point {

    private long instant;

    public TemporalPoint(CoordinateSequence coordinates) {
        this(coordinates, Term.GEOMETRY_FACTORY);
    }

    public TemporalPoint(long timestamp, CoordinateSequence coordinates) {
        this(timestamp, coordinates, Term.GEOMETRY_FACTORY);
    }

    public TemporalPoint(CoordinateSequence coordinates, GeometryFactory factory) {
        this(0, coordinates, factory);
    }

    public TemporalPoint(long timestamp, CoordinateSequence coordinates, GeometryFactory factory) {
        super(coordinates, factory);
        this.instant = timestamp;
    }

    @Override
    public String toText() {
        TWKTWriter writer = new TWKTWriter();
        return writer.write(this);
    }

    @Override
    public String getGeometryType() {
        return "TPoint";
    }

}
