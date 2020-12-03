package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;

public class CustomGeometryFactory extends GeometryFactory {

    private CoordinateSequenceFactory coordinateSequenceFactory;

    public CustomGeometryFactory() {
        this(CoordinateArraySequenceFactory.instance());
    }

    public CustomGeometryFactory(CoordinateSequenceFactory coordinateSequenceFactory) {
        super();
        this.coordinateSequenceFactory = coordinateSequenceFactory;
    }

    public TemporalPoint createTemporalPoint() {
        Point point = this.createPoint();
        return createTemporalPoint(point.getCoordinateSequence(), 0);
    }

    public TemporalPoint createTemporalPoint(CoordinateSequence coordinates, long instant) {
        return new TemporalPoint(instant, coordinates);
    }

    public TemporalLineString createTemporalLineString() {
        Point point = this.createPoint();
        return createTemporalLineString(point.getCoordinateSequence(), new long[]{0});
    }

    public TemporalLineString createTemporalLineString(Coordinate[] coordinates, long[] instants) {
        return createTemporalLineString(coordinates != null ? getCoordinateSequenceFactory().create(coordinates) : null, instants);
    }

    public TemporalLineString createTemporalLineString(CoordinateSequence coordinates, long[] instants) {
        return new TemporalLineString(instants, coordinates);
    }

    public CoordinateSequenceFactory getCoordinateSequenceFactory() {
        return coordinateSequenceFactory;
    }

}
