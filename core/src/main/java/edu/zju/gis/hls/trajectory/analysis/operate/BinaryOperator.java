package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Hu
 * @date 2019/12/9
 * 双目操作符
 **/
public interface BinaryOperator {

  Layer run(Feature f, Layer layer);

  Layer run(Geometry f, Layer layer);

  Layer run(Geometry f, IndexedLayer layer);

  Layer run(Feature f, IndexedLayer layer);

  Layer run(IndexedLayer layer1, IndexedLayer layer2);

}
