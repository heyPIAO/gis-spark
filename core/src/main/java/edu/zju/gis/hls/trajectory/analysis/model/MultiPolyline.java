package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2020/6/25
 **/
@NoArgsConstructor
public class MultiPolyline extends Feature<org.locationtech.jts.geom.MultiLineString> {

  public MultiPolyline(String fid, org.locationtech.jts.geom.MultiLineString geometry, LinkedHashMap<Field, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public MultiPolyline(MultiPolyline f) {
    super(f);
  }

}
