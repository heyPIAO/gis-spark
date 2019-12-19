package edu.zju.gis.hls.trajectory.analysis.model;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/12/16
 **/
public class Term implements Serializable {
  public static CoordinateReferenceSystem DEFAULT_CRS = getDefaultCrs();
  public static int QUADTREE_MIN_Z = 4;
  public static int QUADTREE_MAX_Z = 16;
  public static int QUADTREE_DEFAULT_LEVEL = 12;
  public static int SCREEN_TILE_SIZE = 256;

  public static CoordinateReferenceSystem getDefaultCrs() {
    try {
      return CRS.decode("epsg:4326");
    } catch (FactoryException e) {
      e.printStackTrace();
    }
    return null;
  }

}
