package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/

@Getter
@Setter
public class PolygonFeature extends Feature<Polygon> {

  public PolygonFeature(String fid, Polygon geometry, Map<String, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public PolygonFeature(PolygonFeature f) {
    super(f);
  }

  @Override
  public PolygonFeature shift(double deltaX, double deltaY) {
    Polygon p = this.getGeometry();
    Coordinate[] cs = p.getCoordinates();
    Coordinate[] csr = new Coordinate[cs.length];
    for (int i=0; i<cs.length; i++) {
      Coordinate c = cs[i];
      csr[i] = new Coordinate(c.getX() + deltaX, c.getY() + deltaY);
    }
    this.setGeometry(new GeometryFactory().createPolygon(csr));
    return this;
  }

}
