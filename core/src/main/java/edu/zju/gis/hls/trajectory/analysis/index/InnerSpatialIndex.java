package edu.zju.gis.hls.trajectory.analysis.index;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PartitionIndexedLayer;

/**
 * @author Hu
 * @date 2020/6/23
 * 节点内部索引
 **/
public interface InnerSpatialIndex extends SpatialIndex {
  <L extends Layer, K extends KeyIndexedLayer<L>> PartitionIndexedLayer<L, K> index(K layer);
}
