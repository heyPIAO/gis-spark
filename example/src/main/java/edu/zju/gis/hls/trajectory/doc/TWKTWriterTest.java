package edu.zju.gis.hls.trajectory.doc;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class TWKTWriterTest {

    public static void main(String[] args) {

        Coordinate coordinate = new Coordinate(120.0, 30.00);
        Point point = new GeometryFactory().createPoint(coordinate);
        TemporalPoint p = new TemporalPoint(123218890, point.getCoordinateSequence());
        System.out.println(p.toString());

    }


}
