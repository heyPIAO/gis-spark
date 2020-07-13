package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

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
public class QuadTreeIndexConfig extends IndexConfig {

  private int indexLevel;
  private boolean isClip;

  public QuadTreeIndexConfig() {
    this(Term.QUADTREE_DEFAULT_LEVEL, true);
  }

  public QuadTreeIndexConfig(int level) {
    this(level, true);
  }

  public QuadTreeIndexConfig(int indexLevel, boolean isClip) {
    this.indexLevel = indexLevel;
    this.isClip = isClip;
  }
}
