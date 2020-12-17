package edu.zju.gis.hls.trajectory.doc;

import edu.zju.gis.hls.trajectory.analysis.index.hilbertcurve.HilbertCurve;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.math.BigInteger;

/**
 * @author Hu
 * @date 2020/12/10
 * 希尔伯特曲线编码测试
 **/
public class HilbertCurveTest {

  public static void main(String[] args) throws Exception {
    int indexLevel = 2;
    GeometryFactory gf = new GeometryFactory();
    Coordinate coordinate = new Coordinate(120.00, 30.00);
    Point p = gf.createPoint(coordinate);
    CoordinateReferenceSystem crs = CRS.decode("epsg:4326");
    Envelope e = CrsUtils.getCrsEnvelope(crs);
    double resolutionX = e.getWidth()/Math.pow(2, indexLevel);
    double resolutionY = e.getHeight()/Math.pow(2, indexLevel);
    int x = (int)((p.getX()-e.getMinX())/resolutionX);
    int y = (int)((e.getMaxY() - p.getY())/resolutionY);
    HilbertCurve c = HilbertCurve.bits(indexLevel).dimensions(2);
    BigInteger hilbertNum = c.index(x, y);
    System.out.println("Hilbert curve num is: " + hilbertNum);
  }

}
