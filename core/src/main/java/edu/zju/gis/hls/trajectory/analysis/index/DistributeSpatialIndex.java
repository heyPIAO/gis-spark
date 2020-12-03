package edu.zju.gis.hls.trajectory.analysis.index;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;

/**
 * @author Hu
 * @date 2019/12/16
 * 分布式空间索引，用于数据分片
 **/
public interface DistributeSpatialIndex extends SpatialIndex {
  <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer);
  <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges);
  <L extends Layer, T extends KeyIndexedLayer<L>> T index(L layer, boolean withKeyRanges, int numPartitions);
}
