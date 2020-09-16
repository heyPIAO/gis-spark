package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.geotools.geometry.jts.JTS;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author Hu
 * @date 2019/9/19
 **/
@Getter
@Setter
@NoArgsConstructor
public class Polygon extends Feature<org.locationtech.jts.geom.Polygon> {

  public Polygon(String fid, org.locationtech.jts.geom.Polygon geometry, LinkedHashMap<Field, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public Polygon(org.locationtech.jts.geom.Polygon geometry, LinkedHashMap<Field, Object> attributes) {
    super(geometry, attributes);
  }

  public Polygon(String fid, org.locationtech.jts.geom.Polygon geometry) {
    super(fid, geometry);
  }

  public Polygon(org.locationtech.jts.geom.Polygon geometry) {
    super(geometry);
  }

  public Polygon(Polygon f) {
    super(f);
  }

  public List<Polygon> makeValid(boolean removeHoles) {
    List<org.locationtech.jts.geom.Polygon> polygons = JTS.makeValid(this.geometry, removeHoles);
    List<Polygon> pfs = new ArrayList<>();
    for (org.locationtech.jts.geom.Polygon p: polygons) {
      pfs.add(new Polygon(this.fid, p, this.attributes));
    }
    return pfs;
  }

}
