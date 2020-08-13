package edu.zju.gis.hls.trajectory.analysis.model;

import java.util.LinkedHashMap;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-08-13
 */
public class EmptyGeometry extends Feature<org.geotools.geometry.jts.EmptyGeometry> {

  public EmptyGeometry(String fid, org.geotools.geometry.jts.EmptyGeometry geometry, LinkedHashMap<Field, Object> attributes) {
    super(fid, geometry, attributes);
  }

  public EmptyGeometry(EmptyGeometry f) {
    super(f);
  }
}
