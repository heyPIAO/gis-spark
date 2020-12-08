package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.analysis.proto.TemporalLineString;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.WKTReader2;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.operation.polygonize.Polygonizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class GeometryUtil {

    private static TWKBReader wkbReader = new TWKBReader();
    private static TWKBWriter wkbWriter = new TWKBWriter();
    private static WKTReader2 wktReader = new WKTReader2();
    private static TWKTWriter wktWriter = new TWKTWriter();
    private static CustomGeometryFactory gf = new CustomGeometryFactory();

   public static <T extends Geometry> byte[] toByteArray(T g) {
       return wkbWriter.write(g);
   }

   public static Geometry fromByteArray(byte[] g) {
       try {
           return wkbReader.read(g);
       } catch (ParseException e) {
            e.printStackTrace();
            throw new GISSparkException("Unvalid WKB for Geometry: " + e.getMessage());
       }
   }

   public static Polygon envelopeToPolygon(Envelope e) {
     return (Polygon)gf.toGeometry(e);
   }

   public static Geometry createBBox(Double xmin, Double xmax, Double ymin, Double ymax) {
       return gf.toGeometry(new Envelope(xmin, xmax, ymin, ymax));
   }

   public static Geometry fromWKB(byte[] g) {
       return fromByteArray(g);
   }

    public static byte[] toWKB(Geometry g) {
        return toByteArray(g);
    }

    public static Geometry fromWKT(String wkt) {
        try {
            return wktReader.read(wkt);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new GISSparkException("Unvalid WKT for Geometry: " + e.getMessage());
        }
    }

   public static <T> T createEmptyGeometry(Class<T> t) {

       if (t.equals(Point.class)) {
           return (T) gf.createPoint();
       } if (t.equals(MultiPoint.class)) {
           return (T) gf.createMultiPoint();
       } if (t.equals(LineString.class)) {
           return (T) gf.createLineString();
       } if (t.equals(MultiLineString.class)) {
           return (T) gf.createMultiLineString();
       } if (t.equals(Polygon.class)) {
           return (T) gf.createPolygon();
       } if (t.equals(MultiPolygon.class)) {
           return (T) gf.createMultiPolygon();
       } if (t.equals(TemporalPoint.class)) {
           return (T) gf.createTemporalPoint();
       } if (t.equals(TemporalLineString.class)) {
           return (T) gf.createTemporalLineString();
       }  else {
           throw new GISSparkException("Unvalid geometry type: " + t.getClass().getName());
       }
   }

    @Deprecated
    public static Geometry validedGeom(Geometry g) {
        if(g.isValid()){
            return g;
        }
        List<Polygon> valided = null;
        if (g instanceof org.locationtech.jts.geom.Polygon) {
            valided = new ArrayList<>();
            List<org.locationtech.jts.geom.Polygon> polygons= JTS.makeValid((org.locationtech.jts.geom.Polygon) g, true);
            org.locationtech.jts.geom.Polygon polygon =polygons.get(0);
            for(int j=1;j<polygons.size();j++) {
                polygon = (org.locationtech.jts.geom.Polygon)(polygon.difference(polygons.get(j)));
            }
            valided.add(polygon);
        } else if (g instanceof org.locationtech.jts.geom.MultiPolygon) {
            org.locationtech.jts.geom.MultiPolygon mps = (org.locationtech.jts.geom.MultiPolygon) g;
            int i = mps.getNumGeometries();
            valided = new ArrayList<>();
            for (int m = 0; m < i; m++) {
                List<org.locationtech.jts.geom.Polygon> polygons=JTS.makeValid((org.locationtech.jts.geom.Polygon) mps.getGeometryN(m), true);
                org.locationtech.jts.geom.Polygon polygon =polygons.get(0);
                for(int j=1;j<polygons.size();j++) {
                    polygon = (org.locationtech.jts.geom.Polygon)(polygon.difference(polygons.get(j)));
                }
                valided.add(polygon);
            }
        }

        if (valided != null && valided.size() > 0) {
            org.locationtech.jts.geom.Polygon[] polygons = new org.locationtech.jts.geom.Polygon[valided.size()];
            for (int i=0; i<valided.size(); i++) {
                polygons[i] = valided.get(i);
            }
            return gf.createMultiPolygon(polygons);
        }
        throw new GISSparkException("Unvalid Geometry: " + g.toString());
    }

    /**
     * 用 GDAL 的 makeValid 方法实现不可用图斑的可用重构
     * @param g
     * @return
     */
    public static Geometry validedGeom2(Geometry g) {
        if(g.isValid()){
            return g;
        }
        String wkt = g.toText();
        org.gdal.ogr.Geometry geometry = org.gdal.ogr.Geometry.CreateFromWkt(wkt);
        geometry = geometry.MakeValid();
        wkt = geometry.ExportToWkt();
        WKTReader2 reader = new WKTReader2();
        try {
            return reader.read(wkt);
        } catch (ParseException e) {
            throw new GISSparkException("Unvalid wkt: " + wkt);
        }
    }

    /**
     * StackOverflow 大神之作
     * https://stackoverflow.com/questions/31473553/is-there-a-way-to-convert-a-self-intersecting-polygon-to-a-multipolygon-in-jts
     * @param g
     * @return
     */
    public static Geometry validedGeom3(Geometry g) {
        if(g instanceof Polygon){
            if(g.isValid()){
                g.normalize(); // validate does not pick up rings in the wrong order - this will fix that
                return g; // If the polygon is valid just return it
            }
            Polygonizer polygonizer = new Polygonizer();
            addPolygon((Polygon)g, polygonizer);
            return toPolygonGeometry(polygonizer.getPolygons(), g.getFactory());
        }else if(g instanceof MultiPolygon){
            if(g.isValid()){
                g.normalize(); // validate does not pick up rings in the wrong order - this will fix that
                return g; // If the multipolygon is valid just return it
            }
            Polygonizer polygonizer = new Polygonizer();
            for(int n = g.getNumGeometries(); n-- > 0;){
                addPolygon((Polygon)g.getGeometryN(n), polygonizer);
            }
            return toPolygonGeometry(polygonizer.getPolygons(), g.getFactory());
        }else{
            return g; // In my case, I only care about polygon / multipolygon geometries
        }
    }

    /**
     * Add all line strings from the polygon given to the polygonizer given
     *
     * @param polygon polygon from which to extract line strings
     * @param polygonizer polygonizer
     */
    private static void addPolygon(Polygon polygon, Polygonizer polygonizer){
        addLineString(polygon.getExteriorRing(), polygonizer);
        for(int n = polygon.getNumInteriorRing(); n-- > 0;){
            addLineString(polygon.getInteriorRingN(n), polygonizer);
        }
    }

    /**
     * Add the linestring given to the polygonizer
     *
     * @param lineString line string
     * @param polygonizer polygonizer
     */
    private static void addLineString(LineString lineString, Polygonizer polygonizer){

        if(lineString instanceof LinearRing){ // LinearRings are treated differently to line strings : we need a LineString NOT a LinearRing
            lineString = lineString.getFactory().createLineString(lineString.getCoordinateSequence());
        }

        // unioning the linestring with the point makes any self intersections explicit.
        Point point = lineString.getFactory().createPoint(lineString.getCoordinateN(0));
        Geometry toAdd = lineString.union(point);

        //Add result to polygonizer
        polygonizer.add(toAdd);
    }

    /**
     * Get a geometry from a collection of polygons.
     *
     * @param polygons collection
     * @param factory factory to generate MultiPolygon if required
     * @return null if there were no polygons, the polygon if there was only one, or a MultiPolygon containing all polygons otherwise
     */
    private static Geometry toPolygonGeometry(Collection<Polygon> polygons, GeometryFactory factory){
        switch(polygons.size()){
            case 0:
                return null; // No valid polygons!
            case 1:
                return polygons.iterator().next(); // single polygon - no need to wrap
            default:
                //polygons may still overlap! Need to sym difference them
                Iterator<Polygon> iter = polygons.iterator();
                Geometry ret = iter.next();
                while(iter.hasNext()){
                    ret = ret.symDifference(iter.next());
                }
                return ret;
        }
    }

}
