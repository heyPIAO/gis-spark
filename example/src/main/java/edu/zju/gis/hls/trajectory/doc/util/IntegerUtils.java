package edu.zju.gis.hls.trajectory.doc.util;

/**
 * @author Hu
 * @date 2020/12/21
 **/
public class IntegerUtils {

  public static byte[] IntegerToBytes(int i) {
    byte[] result = new byte[4];
    result[0] = (byte)((i >> 24) & 0xFF);
    result[1] = (byte)((i >> 16) & 0xFF);
    result[2] = (byte)((i >> 8) & 0xFF);
    result[3] = (byte)(i & 0xFF);
    return result;
  }

  public static long bytesToInteger(byte[] b) {
    int value=0;
    for(int i = 0; i < 4; i++) {
      int shift= (3-i) * 8;
      value +=(b[i] & 0xFF) << shift;
    }
    return value;
  }

}
