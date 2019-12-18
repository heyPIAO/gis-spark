package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
@Getter
@Setter
public class Polygon extends Feature<org.locationtech.jts.geom.Polygon> {

  public Polygon(String fid, org.locationtech.jts.geom.Polygon geometry, Map<String, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public Polygon(Polygon f) {
    super(f);
  }

}
