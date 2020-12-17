package edu.zju.gis.hls.trajectory.analysis.index.uniformGrid;

import com.google.common.base.Objects;
import edu.zju.gis.hls.trajectory.analysis.index.IndexConfig;
import edu.zju.gis.hls.trajectory.analysis.index.hilbertcurve.HilbertEncoder;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2020/12/10
 **/
public class UniformGridConfig extends IndexConfig {

  @Getter
  private int level; // 格网划分层数

  @Getter
  @Setter
  private boolean isClip;

  @Getter
  private GridNumEncoder encoder;

  @Getter
  private int dimension;

  public UniformGridConfig() {
    this(8, 2);
  }

  public UniformGridConfig(int level, int dimension) {
    this(level, false, dimension);
  }

  public UniformGridConfig(int level, boolean isClip, int dimension) {
    if (3 < dimension || dimension < 2) throw new GISSparkException("dimension should be in [2,3]");
    this.level = level;
    this.isClip = isClip;
    this.dimension = dimension;
    this.encoder = new HilbertEncoder(level, dimension);
  }

  public void setDimension(int dimension) {
    if (3 < dimension || dimension < 2) throw new GISSparkException("dimension should be in [2,3]");
    this.dimension = dimension;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UniformGridConfig that = (UniformGridConfig) o;
    return level == that.level &&
      isClip == that.isClip &&
      dimension == that.dimension &&
      Objects.equal(encoder, that.encoder);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(level, isClip, encoder, dimension);
  }

}
