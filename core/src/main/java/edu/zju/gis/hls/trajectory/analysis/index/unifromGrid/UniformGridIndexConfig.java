package edu.zju.gis.hls.trajectory.analysis.index.unifromGrid;

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
public class UniformGridIndexConfig extends IndexConfig {

  private int indexLevel;
  private boolean isClip;

  public UniformGridIndexConfig() {
    this(Term.UNIFORMGRID_DEFAULT_LEVEL, true);
  }

  public UniformGridIndexConfig(int level) {
    this(level, true);
  }

  public UniformGridIndexConfig(int indexLevel, boolean isClip) {
    this.indexLevel = indexLevel;
    this.isClip = isClip;
  }
}
