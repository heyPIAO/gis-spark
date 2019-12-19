package edu.zju.gis.hls.test.example;

import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.util.Collection;

/**
 * @author Hu
 * @date 2019/12/18
 **/
public class CoordinateReferenceExample {

  public static void main(String[] args) throws FactoryException {

    CoordinateReferenceSystem crs = CRS.decode("epsg:4326");
    String a = crs.toString();
    String b = crs.toWKT();
    Collection c = crs.getAlias();
    String d = crs.getScope().toString();
    String e = crs.getName().toString();
    String f = crs.getDomainOfValidity().toString();
    String g = crs.getIdentifiers().iterator().next().getCode();

    System.out.println("Hello CRS: " + g);

  }


}
