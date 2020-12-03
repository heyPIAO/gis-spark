package edu.zju.gis.hls.trajectory.analysis.model;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Point;

import java.util.LinkedHashMap;

/**
 * @author Hu
 * @date 2019/11/8
 * 轨迹OD：轨迹开始与结束
 * 目前只是做了语义封装
 **/
@Getter
@Setter
public class TrajectoryOD extends MovingPoint {

  public TrajectoryOD(String fid, Point start, Point end, LinkedHashMap<Field, Object> attributes,LinkedHashMap<Field, Object[]> temporalAttributes, long startTime, long endTime) {
    super(fid, Term.CUSTOM_GEOMETRY_FACTORY.createTemporalLineString(new Coordinate[]{start.getCoordinate(), end.getCoordinate()}, new long[]{startTime, endTime}), attributes, temporalAttributes);
  }

  public TrajectoryOD(TrajectoryOD f) {
    super(f);
  }

  public TemporalLineString getODLine() {
    return this.geometry;
  }

  public TimedPoint getStart() {
    return this.getIndic(0);
  }

  public TimedPoint getEnd() {
    return this.getIndic(1);
  }

}
