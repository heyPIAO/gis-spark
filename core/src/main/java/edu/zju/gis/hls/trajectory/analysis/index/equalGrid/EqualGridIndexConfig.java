package edu.zju.gis.hls.trajectory.analysis.index.equalGrid;

import edu.zju.gis.hls.trajectory.analysis.index.IndexConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2020/12/3
 * TODO 默认划分100个格网
 **/
@Getter
@Setter
public class EqualGridIndexConfig extends IndexConfig {

  private int dimension; // 格网划分的维度，0为lat，1为lon
  private int num; // 格网划分个数
  private boolean isClip;

  public EqualGridIndexConfig() {
    this(0, 100, false);
  }

  public EqualGridIndexConfig(int num) {
    this(0, num, false);
  }

  public EqualGridIndexConfig(int dimension, int num, boolean isClip) {
    this.dimension = dimension;
    this.num = num;
    this.isClip = isClip;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EqualGridIndexConfig that = (EqualGridIndexConfig) o;
    return dimension == that.dimension &&
      num == that.num;
  }

}
