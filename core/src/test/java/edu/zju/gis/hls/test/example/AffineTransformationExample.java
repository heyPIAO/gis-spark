package edu.zju.gis.hls.test.example;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hu
 * @date 2019/11/8
 **/
public class AffineTransformationExample {

  private static final Logger logger = LoggerFactory.getLogger(AffineTransformationExample.class);

  public static void main(String[] args) {
    GeometryFactory gf = new GeometryFactory();
    Coordinate c = new Coordinate(120, 30);
    Point point = gf.createPoint(c);
    AffineTransformation at = new AffineTransformation();
    at.setToTranslation(0, 0);
    Geometry result = at.transform(point);
    System.out.println(result.toText());
    logger.info(result.toText());
  }


}
