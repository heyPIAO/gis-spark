package edu.zju.gis.hls.trajectory.analysis.index.rtree;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2020/12/4
 **/
@Getter
@Setter
public class RTreeIndexLayer<L extends Layer> extends KeyIndexedLayer<L> {

  public RTreeIndexLayer(RTreePartitioner partitioner) {
    super(partitioner);
    this.indexType = IndexType.RTREE;
  }

}
