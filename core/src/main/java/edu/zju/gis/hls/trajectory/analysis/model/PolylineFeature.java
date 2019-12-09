package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
@Getter
@Setter
public class PolylineFeature extends Feature<LineString> {

  public PolylineFeature(String fid, LineString geometry, Map<String, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public PolylineFeature(PolylineFeature f) {
    super(f);
  }

}
