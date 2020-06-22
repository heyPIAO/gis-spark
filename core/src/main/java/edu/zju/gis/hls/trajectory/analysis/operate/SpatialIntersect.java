package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;

/**
 * @author Hu
 * @date 2019/12/9
 * 空间相交
 **/
public class SpatialIntersect implements BinaryOperator {

  @Override
  public <L1 extends Layer, L2 extends IndexedLayer, L3 extends IndexedLayer> L1 run(L2 layer1, L3 layer2) {
    IndexType lt1 = layer1.getIndexType();
    IndexType lt2 = layer2.getIndexType();
    return null;
  }

}
