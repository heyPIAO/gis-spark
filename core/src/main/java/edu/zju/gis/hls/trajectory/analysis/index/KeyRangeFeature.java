package edu.zju.gis.hls.trajectory.analysis.index;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Polygon;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.LinkedHashMap;

/**
 * 各个分区空间区域表征的 Feature
 * @author Hu
 * @date 2020/8/24
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class KeyRangeFeature extends Polygon {

  private int paritionId;

  public KeyRangeFeature(org.locationtech.jts.geom.Polygon geometry, int paritionId) {
    super(geometry);
    this.paritionId = paritionId;
  }

  public KeyRangeFeature(String fid, org.locationtech.jts.geom.Polygon geometry, int paritionId) {
    super(fid, geometry);
    this.paritionId = paritionId;
  }

  public KeyRangeFeature(String fid, org.locationtech.jts.geom.Polygon geometry, LinkedHashMap<Field, Object> attributes, int paritionId) {
    super(fid, geometry, attributes);
    this.paritionId = paritionId;
  }



}
