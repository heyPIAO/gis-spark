package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.IndexConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Hu
 * @date 2020/12/4
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class RTreeIndexConfig extends IndexConfig {

  private boolean isClip;
  private int sampleSize;

  public RTreeIndexConfig() {
    this(false);
  }

  public RTreeIndexConfig(boolean isClip) {
    this(isClip, 1000);
  }

  public RTreeIndexConfig(int sampleSize) {
    this(false, sampleSize);
  }

  public RTreeIndexConfig(boolean isClip, int sampleSize) {
    this.isClip = isClip;
    this.sampleSize = sampleSize;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RTreeIndexConfig that = (RTreeIndexConfig) o;
    return isClip == that.isClip;
  }

}
