package edu.zju.gis.hls.trajectory.analysis.index.equalGrid;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2020/12/3
 **/
@Getter
@Setter
@ToString
public class EqualGrid implements Comparable, Serializable {

  private int index;

  public EqualGrid(int index) {
    this.index = index;
  }

  public static EqualGrid fromString(String gridId) {
    return new EqualGrid(Integer.valueOf(gridId));
  }

  @Override
  public int hashCode() {
    return index;
  }

  @Override
  public int compareTo(Object o) {
    EqualGrid go = (EqualGrid) o;
    return this.index - go.index;
  }

}
