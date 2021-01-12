package edu.zju.gis.hls.trajectory.analysis.index;

import lombok.Getter;

/**
 * @author Hu
 * @date 2019/12/16
 * RECT_GRID： 正方形格网
 * EQUAL_GRID：经度或纬度单向格网
 * UNIFORM_GRID：经纬度双向格网且两个方向的格网数量一致
 * RTREE：基于STRTree的索引
 **/
@Getter
public enum IndexType {

  RECT_GRID("rect_grid", 0), RTREE("rtree", 1), EQUAL_GRID("equal_grid", 5), UNIFORM_GRID("uniform_grid", 2), NN("nn", 3);

  IndexType(String name, int type) {
    this.name = name;
    this.type = type;
  }

  private String name;
  private int type;

  public static IndexType Default() {
    return RECT_GRID;
  }

}
