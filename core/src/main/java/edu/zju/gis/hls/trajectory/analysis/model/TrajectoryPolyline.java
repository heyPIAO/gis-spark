package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.LineString;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
@Getter
@Setter
public class TrajectoryPolyline extends PolylineFeature {

  @Getter
  @Setter
  private long startTime;

  @Getter
  @Setter
  private long endTime;

  public TrajectoryPolyline(String fid, LineString geometry, Map<String, Object> attributes, long startTime, long endTime) {
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
    return geometryMap;
  }


}
