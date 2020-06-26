package edu.zju.gis.hls.trajectory.analysis.model;

import java.util.Map;

/**
 * @author Hu
 * @date 2020/6/25
 **/
public class MultiPoint extends Feature<org.locationtech.jts.geom.MultiPoint> {

  public MultiPoint(String fid, org.locationtech.jts.geom.MultiPoint geometry, Map<Field, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public MultiPoint(MultiPoint f) {
    super(f);
  }

}