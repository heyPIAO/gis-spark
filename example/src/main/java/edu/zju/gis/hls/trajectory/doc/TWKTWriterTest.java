package edu.zju.gis.hls.trajectory.doc;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import edu.zju.gis.hls.trajectory.analysis.util.TWKBReader;
import edu.zju.gis.hls.trajectory.analysis.util.TWKBWriter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;

import java.util.Arrays;

public class TWKTWriterTest {

    public static void main(String[] args) throws ParseException {

        System.out.println("==== Temporal Point Wkt Encode ====");
        Coordinate coordinate = new Coordinate(120.0, 30.00);
        Point point = new GeometryFactory().createPoint(coordinate);
        TemporalPoint p = new TemporalPoint(123218890, point.getCoordinateSequence());
        System.out.println(p.toString());


        System.out.println("==== Temporal LineString Wkt Encode ====");
        Coordinate[] coordinates = new Coordinate[2];
        Coordinate coordinate2 = new Coordinate(120.4, 30.40);
        coordinates[0] = coordinate;
        coordinates[1] = coordinate2;
        CoordinateSequence cs = new CoordinateArraySequence(coordinates);
        TemporalLineString tls = new TemporalLineString(new long[]{123218890, 123218894}, cs);
        System.out.println(tls.toString());

        System.out.println("==== Temporal Point Wkb Encode ====");
        TWKBWriter writer = new TWKBWriter();
        byte[] bytes = writer.write(p);
        System.out.println(Arrays.toString(bytes));

        System.out.println("==== Temporal LineString Wkb Encode ====");
        byte[] linebytes = writer.write(tls);
        System.out.println(Arrays.toString(linebytes));

        System.out.println("==== Temporal Point Wkb Decode ====");
        TWKBReader reader = new TWKBReader();
        TemporalPoint tp = (TemporalPoint) reader.read(bytes);
        System.out.println(tp.toString());

        System.out.println("==== Temporal LineString Wkb Decode ====");
        TemporalLineString tl = (TemporalLineString) reader.read(linebytes);
        System.out.println(tl.toString());

    }


}
