package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;

import java.io.Serializable;

/**
 * 图层单目操作
 * @author Hu, Keran Sun (katus)
 * @date 2019/12/9
 * @version 2.0, 2020-08-13
 **/
public interface Operator extends Serializable {

  Layer run(Layer layer);

  <T extends KeyIndexedLayer> Layer run(T layer);

}
