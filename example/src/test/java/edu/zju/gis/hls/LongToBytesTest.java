package edu.zju.gis.hls;

import com.google.common.primitives.Bytes;
import edu.zju.gis.hls.trajectory.doc.util.LongUtils;

import java.math.BigInteger;

/**
 * @author Hu
 * @date 2020/12/18
 **/
public class LongToBytesTest {

  public static void main(String[] args) {

    long l1 = 1000;
    long l2 = 2000;

    byte[] gridHilbertIndex = LongUtils.LongToBytes(l1);
    byte[] inGridIndex = LongUtils.LongToBytes(l2);
    byte[] index = Bytes.concat(gridHilbertIndex, inGridIndex);
    BigInteger bigInteger = new BigInteger(index);
    System.out.println(bigInteger);
    byte[] r = bigInteger.toByteArray();
  }

}
