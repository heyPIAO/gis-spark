package edu.zju.gis.hls.trajectory.analysis.model;

import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.*;

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

  public static Field FIELD_DEFAULT_SHAPE =  new Field(SHAPE_FIELD.name(), SHAPE_FIELD.name().toLowerCase(), Geometry.class.getName(), 0, -1, SHAPE_FIELD);
  public static Field FIELD_DEFAULT_ID = new Field(ID_FIELD.name(), ID_FIELD.name().toLowerCase(), String.class.getName(), 0, -99, SHAPE_FIELD);
  public static Field FIELD_DEFAULT_TIME = new Field(TIME_FIELD.name(), TIME_FIELD.name().toLowerCase(), Long.class.getName(), 0, -99, TIME_FIELD);
  public static Field FIELD_DEFAULT_START_TIME = new Field(START_TIME_FIELD.name(), START_TIME_FIELD.name().toLowerCase(), Long.class.getName(), 0, -99, START_TIME_FIELD);
  public static Field FIELD_DEFAULT_END_TIME = new Field(END_TIME_FIELD.name(), END_TIME_FIELD.name().toLowerCase(), Long.class.getName(), 0, -99, END_TIME_FIELD);

  public static CoordinateReferenceSystem getDefaultCrs() {
    try {
      return CRS.decode("epsg:4326");
    } catch (FactoryException e) {
      e.printStackTrace();
    }
    return null;
  }

}
