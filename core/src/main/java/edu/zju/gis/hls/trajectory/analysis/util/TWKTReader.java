package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import org.geotools.geometry.jts.WKTReader2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

/**
 * @author Hu
 * @date 2020/12/21
 * TODO Read TPOINT and TLINESTRING
 **/
public class TWKTReader extends WKTReader2 {

  public TemporalLineString readTemporalLineString(String line) {
    int index = line.indexOf("(");
    line = line.substring(index+1, line.length()-1);
    String[] ps = line.split(",");
    Coordinate[] coordinates = new Coordinate[ps.length];
    long[] instants = new long[ps.length];
    for (int i=0; i<ps.length; i++) {
      String[] p = ps[i].trim().split(" ");
      coordinates[i] = new Coordinate(Double.valueOf(p[0]), Double.valueOf(p[1]));
      instants[i] = Long.valueOf(p[2]);
    }
    return new TemporalLineString(instants, new CoordinateArraySequence(coordinates));
  }

}
