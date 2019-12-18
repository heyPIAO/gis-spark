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
 * 轨迹OD：轨迹开始与结束
 * 目前只是做了语义封装
 **/
@Getter
@Setter
public class TrajectoryOD extends TrajectoryPolyline {

  public TrajectoryOD(String fid, Point start, Point end, Map<String, Object> attributes, long startTime, long endTime) {
    super(fid, new GeometryFactory().createLineString(new Coordinate[]{start.getCoordinate(), end.getCoordinate()}), attributes, startTime, endTime);
  }

  public TrajectoryOD(TrajectoryOD f) {
    super(f);
  }

  public LineString getODLine() {
    return this.geometry;
  }

  public Point getStart() {
    return this.geometry.getStartPoint();
  }

  public Point getEnd() {
    return this.geometry.getEndPoint();
  }

  @Override
  protected Map<String, Object> getGeometryMap() {
    Map<String, Object> r = super.getGeometryMap();
    r.put("type", "od");
    return r;
  }

}
