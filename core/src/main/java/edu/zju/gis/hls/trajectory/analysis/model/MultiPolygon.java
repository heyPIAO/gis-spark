package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2020/6/25
 **/
@NoArgsConstructor
public class MultiPolygon extends Feature<org.locationtech.jts.geom.MultiPolygon> {

  public MultiPolygon(String fid, org.locationtech.jts.geom.MultiPolygon geometry, LinkedHashMap<Field, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public MultiPolygon(String fid, org.locationtech.jts.geom.MultiPolygon geometry) {
    super(fid, geometry);
  }

  public MultiPolygon(MultiPolygon f) {
    super(f);
  }

}
