package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/12/25
 * 时序图斑
 * 相较于TrajectoryPolygon，时序图斑只是一个有时间范围的图斑，表征了其存在时间，并没有空间上的首尾
 * TODO Add a trajectory polygon feature type to the who framework
 **/
public class TrajectoryPolygon extends Polygon {

  private static final Logger logger = LoggerFactory.getLogger(TrajectoryPolygon.class);

  @Getter
  @Setter
  protected long startTime;

  @Getter
  @Setter
  protected long endTime;

  public TrajectoryPolygon(String fid, org.locationtech.jts.geom.Polygon geometry, Map<Field, Object> attributes, long startTime, long endTime) {
    super(fid, geometry, attributes);
    this.startTime = startTime;
    this.endTime = endTime;
  }

  public TrajectoryPolygon(TrajectoryPolygon f) {
    super(f);
    this.startTime = f.getStartTime();
    this.endTime = f.getEndTime();
  }

  @Override
  protected Map<String, Object> getGeometryMap() {
    Map<String, Object> geometryMap = super.getGeometryMap();
    geometryMap.put("startTime", this.startTime);
    geometryMap.put("endTime", this.endTime);
    geometryMap.put("type", "TPolygon"); // TPolygon: Timed-Polygon
    return geometryMap;
  }

}
