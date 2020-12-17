package edu.zju.gis.hls.trajectory.analysis.index.uniformGrid;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Hu
 * @date 2020/12/10
 **/
@Getter
@Setter
@Slf4j
public class UniformGridIndexLayer  <L extends Layer> extends KeyIndexedLayer<L> {

  public UniformGridIndexLayer(UniformGridPartitioner partitioner) {
    super(partitioner);
    this.indexType = IndexType.UNIFORM_GRID;
  }

}
