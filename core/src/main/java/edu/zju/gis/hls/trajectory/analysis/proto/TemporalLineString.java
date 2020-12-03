package edu.zju.gis.hls.trajectory.analysis.proto;

import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.TWKTWriter;
import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.*;

@Getter
@Setter
public class TemporalLineString extends LineString {

    private long[] instants;

    public TemporalLineString() {
        this(Term.COORDINATE_SEQUENCE_FACTORY.create(new Coordinate[]{}));
    }

    public TemporalLineString(CoordinateSequence points) {
        this(new long[points.size()], points, Term.GEOMETRY_FACTORY);
    }

    public TemporalLineString(CoordinateSequence points, GeometryFactory factory) {
        this(new long[points.size()], points, factory);
    }


    public TemporalLineString(long[] instants, CoordinateSequence points) {
        this(instants, points, Term.GEOMETRY_FACTORY);
    }

    public TemporalLineString(long[] instants, CoordinateSequence points, GeometryFactory factory) {
        super(points, factory);
        this.instants = instants;
    }

    public TemporalPoint getTemporalPoint(int index) {
        Point p = this.getPointN(index);
        long instant = instants[index];
        return new TemporalPoint(instant, p.getCoordinateSequence());
    }

    public long getTimeResolution() {
        return instants[1] - instants[0];
    }


    @Override
    public String toText() {
        TWKTWriter writer = new TWKTWriter();
        return writer.write(this);
    }

    @Override
    public String getGeometryType() {
        return "TLineString";
    }

}
