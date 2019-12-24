package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 * 轨迹线：相较于普通的Polyline，多开始时间，结束时间两个属性
 **/
@Getter
@Setter
public class TrajectoryPolyline extends Polyline {

  private static final Logger logger = LoggerFactory.getLogger(TrajectoryPolyline.class);

  @Getter
  @Setter
  protected long startTime;

  @Getter
  @Setter
  protected long endTime;

  public TrajectoryPolyline(String fid, LineString geometry, Map<Field, Object> attributes, long startTime, long endTime) {
    super(fid, geometry, attributes);
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public TrajectoryPolyline(TrajectoryPolyline f) {
    super(f);
    this.startTime = f.getStartTime();
    this.endTime = f.getEndTime();
  }

  @Override
  protected Map<String, Object> getGeometryMap() {
    Map<String, Object> geometryMap = super.getGeometryMap();
    geometryMap.put("startTime", this.startTime);
    geometryMap.put("endTime", this.endTime);
    geometryMap.put("type", "TLineString"); // TLineString: Timed-LineString
    return geometryMap;
  }

  public TrajectoryOD extractOD() {
    GeometryFactory gf = new GeometryFactory();
    LineString g = this.getGeometry();
    Coordinate[] coordinates = g.getCoordinates();
    if (coordinates.length < 2) {
      logger.warn("Unvalid trajectory line: coordinates length smaller than 2");
      return null;
    }
    org.locationtech.jts.geom.Point start = gf.createPoint(coordinates[0]);
    Point end = gf.createPoint(coordinates[coordinates.length-1]);
    return new TrajectoryOD(this.fid, start, end, this.getAttributes(), startTime, endTime);
  }

}
