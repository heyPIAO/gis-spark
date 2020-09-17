package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/12/9
 * 单目操作符
 **/
public interface Operator extends Serializable {

  Layer operate(Layer layer);

  <T extends KeyIndexedLayer> Layer operate(T layer);

}
