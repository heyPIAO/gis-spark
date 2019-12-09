package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/

@Getter
@Setter
public class PointFeature extends Feature<Point> {

  public PointFeature(String fid, Point geometry, Map<String, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public PointFeature(PointFeature f) {
    super(f);
  }

}
