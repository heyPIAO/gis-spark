package edu.zju.gis.hls.trajectory.analysis.index.rectGrid;

import edu.zju.gis.hls.trajectory.analysis.index.IndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2020/6/23
 **/
@Getter
@Setter
public class RectGridIndexConfig extends IndexConfig {

  private int indexLevel;
  private boolean isClip;

  public RectGridIndexConfig() {
    this(Term.UNIFORMGRID_DEFAULT_LEVEL, true);
  }

  public RectGridIndexConfig(int level) {
    this(level, true);
  }

  public RectGridIndexConfig(int indexLevel, boolean isClip) {
    this.indexLevel = indexLevel;
    this.isClip = isClip;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RectGridIndexConfig that = (RectGridIndexConfig) o;
    return indexLevel == that.indexLevel &&
      isClip == that.isClip;
  }

}
