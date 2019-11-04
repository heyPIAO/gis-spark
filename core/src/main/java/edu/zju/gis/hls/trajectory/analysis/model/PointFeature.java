package edu.zju.gis.hls.trajectory.analysis.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import lombok.Setter;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.io.IOException;
import java.util.HashMap;
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

  @Override
  public PointFeature shift(double deltaX, double deltaY) {
    Double x = this.getGeometry().getX() + deltaX;
    Double y = this.getGeometry().getY() + deltaY;
    Coordinate c = new Coordinate(x, y);
    this.setGeometry(new GeometryFactory().createPoint(c));
    return this;
  }

}
