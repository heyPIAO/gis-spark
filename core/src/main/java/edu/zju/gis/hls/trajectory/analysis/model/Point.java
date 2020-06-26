package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/

@Getter
@Setter
public class Point extends Feature<org.locationtech.jts.geom.Point> {

  public Point(String fid, org.locationtech.jts.geom.Point geometry, LinkedHashMap<Field, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public Point(Point f) {
    super(f);
  }

}
