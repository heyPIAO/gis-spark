package edu.zju.gis.hls.trajectory.analysis.index.hilbertcurve;

import com.google.common.base.Objects;
import edu.zju.gis.hls.trajectory.analysis.index.uniformGrid.GridNumEncoder;

import java.math.BigInteger;

/**
 * @author Hu
 * @date 2020/12/10
 **/
public class HilbertEncoder extends GridNumEncoder {

  private HilbertCurve c;

  public HilbertEncoder(int level, int dimension) {
    this.c = HilbertCurve.bits(level).dimensions(dimension);
  }

  @Override
  public BigInteger encode(long x, long y) {
    return c.index(x, y);
  }

  @Override
  public long[] decode(BigInteger id) {
    return c.point(id);
  }

  @Override
  public int getColNum() {
    return Double.valueOf(Math.pow(2, this.c.getBits())).intValue();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HilbertEncoder that = (HilbertEncoder) o;
    return Objects.equal(c, that.c);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(c);
  }

}
