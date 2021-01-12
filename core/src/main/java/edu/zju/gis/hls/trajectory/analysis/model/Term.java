package edu.zju.gis.hls.trajectory.analysis.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import edu.zju.gis.hls.trajectory.analysis.index.rectGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.util.CustomGeometryFactory;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.serializer.CRSJsonSerializer;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.CoordinateArraySequenceFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.*;

/**
 * @author Hu
 * @date 2019/12/16
 **/
public class Term implements Serializable {

  public static String WKT_4528 = null;
  public static String WKT_3857 = null;

  // TODO 改到配置文件中
  static {

    WKT_4528 = "PROJCS[\"CGCS2000 / 3-degree Gauss-Kruger zone 40\",GEOGCS[\"China Geodetic Coordinate System 2000\",DATUM[\"China_2000\",SPHEROID[\"CGCS2000\",6378137,298.257222101,AUTHORITY[\"EPSG\",\"1024\"]],AUTHORITY[\"EPSG\",\"1043\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AUTHORITY[\"EPSG\",\"4490\"]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"latitude_of_origin\",0],PARAMETER[\"central_meridian\",120],PARAMETER[\"scale_factor\",1],PARAMETER[\"false_easting\",40500000],PARAMETER[\"false_northing\",0],UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],AUTHORITY[\"EPSG\",\"4528\"]]";

    WKT_3857 = "PROJCS[\"WGS_1984_Web_Mercator_Auxiliary_Sphere\", \n" +
      "  GEOGCS[\"GCS_WGS_1984\", \n" +
      "    DATUM[\"D_WGS_1984\", \n" +
      "      SPHEROID[\"WGS_1984\", 6378137.0, 298.257223563]], \n" +
      "    PRIMEM[\"Greenwich\", 0.0], \n" +
      "    UNIT[\"degree\", 0.017453292519943295], \n" +
      "    AXIS[\"Longitude\", EAST], \n" +
      "    AXIS[\"Latitude\", NORTH]], \n" +
      "  PROJECTION[\"Popular Visualisation Pseudo Mercator\"], \n" +
      "  PARAMETER[\"semi_minor\", 6378137.0], \n" +
      "  PARAMETER[\"latitude_of_origin\", 0.0], \n" +
      "  PARAMETER[\"central_meridian\", 0.0], \n" +
      "  PARAMETER[\"scale_factor\", 1.0], \n" +
      "  PARAMETER[\"false_easting\", 0.0], \n" +
      "  PARAMETER[\"false_northing\", 0.0], \n" +
      "  UNIT[\"m\", 1.0], \n" +
      "  AXIS[\"x\", EAST], \n" +
      "  AXIS[\"y\", NORTH]]";

  }

  public static CoordinateReferenceSystem DEFAULT_CRS = DEFAULT_CRS();
  public static int QUADTREE_MIN_Z = 4;
  public static int QUADTREE_MAX_Z = 16;
  public static int UNIFORMGRID_DEFAULT_LEVEL = 10;
  public static int SCREEN_TILE_SIZE = 256;

  public static Integer FIELD_NOT_EXIST = -99;
  public static Integer FIELD_LAST = -1;
  public static Integer FIELD_EXIST = 99;
  public static Integer FIELD_LENGTH = 255;

  public static Field FIELD_DEFAULT_SHAPE =  new Field(SHAPE_FIELD.name(), SHAPE_FIELD.name().toLowerCase(), Geometry.class.getName(), 0, FIELD_LAST, SHAPE_FIELD);
  public static Field FIELD_DEFAULT_ID = new Field(ID_FIELD.name(), ID_FIELD.name().toLowerCase(), String.class.getName(), 0, FIELD_NOT_EXIST, ID_FIELD);
  public static Field FIELD_DEFAULT_TIME = new Field(TIME_FIELD.name(), TIME_FIELD.name().toLowerCase(), Long.class.getName(), 0, FIELD_NOT_EXIST, TIME_FIELD);
  // Geometry 转 GeoJSON 的坐标精度
  public static Integer GEOMETRY_JSON_DECIMAL = 9;

  public static Gson GSON_CONTEXT = new GsonBuilder().registerTypeAdapter(CoordinateReferenceSystem.class, new CRSJsonSerializer())
    .registerTypeAdapter(CoordinateReferenceSystem.class, new CRSJsonSerializer.CRSJsonDeserializer()).create();
  public static GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  public static CustomGeometryFactory CUSTOM_GEOMETRY_FACTORY = new CustomGeometryFactory();
  public static CoordinateSequenceFactory COORDINATE_SEQUENCE_FACTORY = CoordinateArraySequenceFactory.instance();

  public static String DAUM_KEY = "DAUM";

  // 图层元数据信息的属性名称
  public static Field LAYER_META_COUNT_FIELD = new Field("LAYER_COUNT", "count", Long.class.getName(), 0, FIELD_EXIST, NORMAL_FIELD);

  public static CoordinateReferenceSystem DEFAULT_CRS() {
    try {
//       return CRS.parseWKT(WKT_4528);
      return CRS.decode("epsg:4326");
    } catch (FactoryException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static Integer getEpsgCode(CoordinateReferenceSystem crs) {
    try {
      return CRS.lookupEpsgCode(crs, true);
    } catch (FactoryException e) {
      throw new GISSparkException("Cannot find epsg code for Coordinate Reference System: " + crs.toWKT());
    }
  }

  public final static PyramidConfig DEFAULT_PYRAMID_CONFIG () throws FactoryException {
    // TODO 默认为 3857 的 Pyramid Config
    return new PyramidConfig.PyramidConfigBuilder()
      .setBaseMapEnv(-20026376.39, 20026376.39, -20048966.10, 20048966.10)
      .setCrs(CRS.parseWKT(WKT_3857))
      .setZLevelRange(UNIFORMGRID_DEFAULT_LEVEL-1, UNIFORMGRID_DEFAULT_LEVEL)
      .build(true);
  }

}
