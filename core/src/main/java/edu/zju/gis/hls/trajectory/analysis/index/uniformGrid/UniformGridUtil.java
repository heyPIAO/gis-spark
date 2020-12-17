package edu.zju.gis.hls.trajectory.analysis.index.uniformGrid;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2020/12/10
 * 单层横纵格网数量一致
 **/
public class UniformGridUtil implements Serializable {

  /**
   * grid number start from the minimum
   * @param f
   * @param min
   * @param resolution
   * @return
   */
  public static int getGridNum(double f, double min, double resolution) {
    return (int)((f-min)/resolution);
  }

  /**
   * grid number start from the maximum
   * @param f
   * @param max
   * @param resolution
   * @return
   */
  public static int getReverseGridNum(double f, double max, double resolution) {
    return (int)((max-f)/resolution);
  }

}
