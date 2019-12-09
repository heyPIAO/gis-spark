package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/11/8
 **/
@Getter
@Setter
public class TrajectoryOD extends TrajectoryPolyline {

  private Point start;

  private Point end;

  public TrajectoryOD(String fid, Point start, Point end, Map<String, Object> attributes, long startTime, long endTime) {
    super(fid, new GeometryFactory().createLineString(new Coordinate[]{start.getCoordinate(), end.getCoordinate()}), attributes, startTime, endTime);
    this.start = start;
    this.end = end;
  }

  public TrajectoryOD(TrajectoryOD f) {
    super(f);
    this.start = f.start;
    this.end = f.end;
  }

  public LineString getODLine() {
    return this.geometry;
  }

}
