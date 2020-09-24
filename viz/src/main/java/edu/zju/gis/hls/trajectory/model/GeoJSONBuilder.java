package edu.zju.gis.hls.trajectory.model;

import edu.zju.gis.hls.trajectory.model.RoundingUtil;
import net.sf.json.JSONException;
import net.sf.json.util.JSONBuilder;
import org.geotools.referencing.CRS;
import org.geotools.util.Converters;
import org.locationtech.jts.geom.*;

import java.io.Writer;
import java.util.*;

/**
 * @author Hu
 * @date 2019/9/5
 **/
public class GeoJSONBuilder extends JSONBuilder {
  private CRS.AxisOrder axisOrder;
  private int numDecimals;
  private boolean encodeMeasures;
  protected static final int POINT = 1;
  protected static final int LINESTRING = 2;
  protected static final int POLYGON = 3;
  protected static final int MULTIPOINT = 4;
  protected static final int MULTILINESTRING = 5;
  protected static final int MULTIPOLYGON = 6;
  protected static final int MULTIGEOMETRY = 7;

  public GeoJSONBuilder(Writer w) {
    super(w);
    this.axisOrder = CRS.AxisOrder.EAST_NORTH;
    this.numDecimals = 6;
    this.encodeMeasures = false;
  }

  public JSONBuilder writeGeom(Geometry geometry) throws JSONException {
    this.object();
    this.key("type");
    this.value(getGeometryName(geometry));
    int geometryType = getGeometryType(geometry);
    if (geometryType != 7) {
      this.key("coordinates");
      int i;
      int n;
      switch(geometryType) {
        case 1:
          Point point = (Point)geometry;
          this.writeCoordinate(point);
          break;
        case 2:
          this.writeCoordinates(((LineString)geometry).getCoordinateSequence());
          break;
        case 3:
          this.writePolygon((Polygon)geometry);
          break;
        case 4:
          this.array();
          i = 0;

          for(n = geometry.getNumGeometries(); i < n; ++i) {
            this.writeCoordinate((Point)geometry.getGeometryN(i));
          }

          this.endArray();
          break;
        case 5:
          this.array();
          i = 0;

          for(n = geometry.getNumGeometries(); i < n; ++i) {
            this.writeCoordinates(((LineString)geometry.getGeometryN(i)).getCoordinateSequence());
          }

          this.endArray();
          break;
        case 6:
          this.array();
          i = 0;

          for(n = geometry.getNumGeometries(); i < n; ++i) {
            this.writePolygon((Polygon)geometry.getGeometryN(i));
          }

          this.endArray();
      }
    } else {
      this.writeGeomCollection((GeometryCollection)geometry);
    }

    return this.endObject();
  }

  private JSONBuilder writeGeomCollection(GeometryCollection collection) {
    this.key("geometries");
    this.array();
    int i = 0;

    for(int n = collection.getNumGeometries(); i < n; ++i) {
      this.writeGeom(collection.getGeometryN(i));
    }

    return this.endArray();
  }

  private JSONBuilder writeCoordinate(Point point) throws JSONException {
    CoordinateSequence coordinates = point.getCoordinateSequence();
    double m = this.encodeMeasures ? coordinates.getM(0) : 0.0D / 0.0;
    return this.writeCoordinate(coordinates.getX(0), coordinates.getY(0), coordinates.getZ(0), m);
  }

  private JSONBuilder writeCoordinates(CoordinateSequence coordinates) throws JSONException {
    this.array();

    for(int i = 0; i < coordinates.size(); ++i) {
      double m = this.encodeMeasures ? coordinates.getM(i) : 0.0D / 0.0;
      this.writeCoordinate(coordinates.getX(i), coordinates.getY(i), coordinates.getZ(i), m);
    }

    return this.endArray();
  }

  private JSONBuilder writeCoordinate(double x, double y, double z, double m) {
    this.array();
    if (this.axisOrder == CRS.AxisOrder.NORTH_EAST) {
      if (!Double.isNaN(y)) {
        this.roundedValue(y);
      }
      this.roundedValue(x);
    } else {
      this.roundedValue(x);
      if (!Double.isNaN(y)) {
        this.roundedValue(y);
      }
    }

    z = Double.isNaN(z) && !Double.isNaN(m) ? 0.0D : z;
    if (!Double.isNaN(z)) {
      this.roundedValue(z);
    }

    if (!Double.isNaN(m)) {
      this.roundedValue(m);
    }

    return this.endArray();
  }

  private void roundedValue(double value) {
    super.value(RoundingUtil.round(value, this.numDecimals));
  }

  protected JSONBuilder writeBoundingBox(Envelope env) {
    this.key("bbox");
    this.array();
    if (this.axisOrder == CRS.AxisOrder.NORTH_EAST) {
      this.roundedValue(env.getMinY());
      this.roundedValue(env.getMinX());
      this.roundedValue(env.getMaxY());
      this.roundedValue(env.getMaxX());
    } else {
      this.roundedValue(env.getMinX());
      this.roundedValue(env.getMinY());
      this.roundedValue(env.getMaxX());
      this.roundedValue(env.getMaxY());
    }

    return this.endArray();
  }

  private void writePolygon(Polygon geometry) throws JSONException {
    this.array();
    this.writeCoordinates(geometry.getExteriorRing().getCoordinateSequence());
    int i = 0;

    for(int ii = geometry.getNumInteriorRing(); i < ii; ++i) {
      this.writeCoordinates(geometry.getInteriorRingN(i).getCoordinateSequence());
    }

    this.endArray();
  }

  public static String getGeometryName(Geometry geometry) {
    if (geometry instanceof Point) {
      return "Point";
    } else if (geometry instanceof LineString) {
      return "LineString";
    } else if (geometry instanceof Polygon) {
      return "Polygon";
    } else if (geometry instanceof MultiPoint) {
      return "MultiPoint";
    } else if (geometry instanceof MultiLineString) {
      return "MultiLineString";
    } else if (geometry instanceof MultiPolygon) {
      return "MultiPolygon";
    } else if (geometry instanceof GeometryCollection) {
      return "GeometryCollection";
    } else {
      throw new IllegalArgumentException("Unknown geometry type " + geometry.getClass());
    }
  }

  public static int getGeometryType(Geometry geometry) {
    if (geometry instanceof Point) {
      return 1;
    } else if (geometry instanceof LineString) {
      return 2;
    } else if (geometry instanceof Polygon) {
      return 3;
    } else if (geometry instanceof MultiPoint) {
      return 4;
    } else if (geometry instanceof MultiLineString) {
      return 5;
    } else if (geometry instanceof MultiPolygon) {
      return 6;
    } else if (geometry instanceof GeometryCollection) {
      return 7;
    } else {
      throw new IllegalArgumentException("Unable to determine geometry type " + geometry.getClass());
    }
  }

  public JSONBuilder writeList(List list) {
    this.array();
    Iterator var2 = list.iterator();

    while(var2.hasNext()) {
      Object o = var2.next();
      this.value(o);
    }

    return this.endArray();
  }

  public JSONBuilder writeMap(Map map) {
    this.object();
    Iterator var2 = map.keySet().iterator();

    while(var2.hasNext()) {
      Object k = var2.next();
      this.key(k.toString());
      this.value(map.get(k));
    }

    return this.endObject();
  }

  public GeoJSONBuilder value(Object value) {
    if (value == null) {
      super.value(value);
    } else if (value instanceof Geometry) {
      this.writeGeom((Geometry)value);
    } else if (value instanceof List) {
      this.writeList((List)value);
    } else if (value instanceof Map) {
      this.writeMap((Map)value);
    } else {
      if (value instanceof Date || value instanceof Calendar) {
        value = Converters.convert(value, String.class);
      }

      super.value(value);
    }

    return this;
  }

  public void setAxisOrder(CRS.AxisOrder axisOrder) {
    this.axisOrder = axisOrder;
  }

  public void setNumberOfDecimals(int numberOfDecimals) {
    this.numDecimals = numberOfDecimals;
  }

  public void setEncodeMeasures(boolean encodeMeasures) {
    this.encodeMeasures = encodeMeasures;
  }
}
