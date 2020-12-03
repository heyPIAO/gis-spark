package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2020/6/25
 **/
@NoArgsConstructor
public class MultiPoint extends Feature<org.locationtech.jts.geom.MultiPoint> {

  public MultiPoint(String fid, org.locationtech.jts.geom.MultiPoint geometry, LinkedHashMap<Field, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public MultiPoint(MultiPoint f) {
    super(f);
  }

}