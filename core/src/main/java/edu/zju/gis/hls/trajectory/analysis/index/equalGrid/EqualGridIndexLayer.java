package edu.zju.gis.hls.trajectory.analysis.index.equalGrid;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Hu
 * @date 2020/12/3
 * 均匀单向划分索引
 **/
@Getter
@Setter
@Slf4j
public class EqualGridIndexLayer <L extends Layer> extends KeyIndexedLayer<L> {

  public EqualGridIndexLayer(EqualGridPartitioner partitioner) {
    super(partitioner);
    this.indexType = IndexType.EQUAL_GRID;
  }

}
