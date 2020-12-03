package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;
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
