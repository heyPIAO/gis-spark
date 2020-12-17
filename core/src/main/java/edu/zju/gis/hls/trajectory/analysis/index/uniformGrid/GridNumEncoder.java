package edu.zju.gis.hls.trajectory.analysis.index.uniformGrid;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * @author Hu
 * @date 2020/12/10
 **/
public abstract class GridNumEncoder implements Serializable {

  public abstract BigInteger encode(long x, long y);

  public abstract long[] decode(BigInteger id);

  public abstract int getColNum();

}
