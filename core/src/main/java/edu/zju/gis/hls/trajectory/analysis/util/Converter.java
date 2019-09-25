package edu.zju.gis.hls.trajectory.analysis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/9/21
 **/
public class Converter implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(Converter.class);

  public static Object convert(String v, Class<?> c) {

    if (c.equals(String.class)) {
      return v;
    } else if (c.equals(Integer.class) || c.equals(int.class)) {
      return Integer.valueOf(v);
    } else if (c.equals(Double.class) || c.equals(double.class)) {
      return Double.valueOf(v);
    } else if (c.equals(Float.class) || c.equals(float.class)) {
      return Float.valueOf(v);
    } else if (c.equals(Long.class) || c.equals(long.class)) {
      return Long.valueOf(v);
    } else {
      logger.error("Unsupport attribute type: " + c.getName());
      return v;
    }

  }


}
