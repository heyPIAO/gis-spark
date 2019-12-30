package edu.zju.gis.hls.trajectory.analysis.index;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;

/**
 * @author Hu
 * @date 2019/12/16
 * 空间索引
 **/
public interface SpatialIndex {
  <L extends Layer, T extends IndexedLayer<L>> T index(L layer);
}
