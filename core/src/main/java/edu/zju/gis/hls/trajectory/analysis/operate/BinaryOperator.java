package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;

/**
 * @author Hu
 * @date 2019/12/9
 * 双目操作符
 **/
public interface BinaryOperator {
  <L1 extends Layer, L2 extends IndexedLayer, L3 extends IndexedLayer> L1 run(L2 layer1, L3 layer2);
}
