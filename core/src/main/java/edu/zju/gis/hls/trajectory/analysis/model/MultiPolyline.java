package edu.zju.gis.hls.trajectory.analysis.model;

import java.util.Map;

/**
 * @author Hu
 * @date 2020/6/25
 **/
public class MultiPolyline extends Feature<org.locationtech.jts.geom.MultiLineString> {

  public MultiPolyline(String fid, org.locationtech.jts.geom.MultiLineString geometry, Map<Field, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public MultiPolyline(MultiPolyline f) {
    super(f);
  }

}
