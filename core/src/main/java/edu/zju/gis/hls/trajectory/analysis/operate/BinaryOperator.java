package edu.zju.gis.hls.trajectory.analysis.operate;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;

/**
 * @author Hu
 * @date 2019/12/9
 * 图层双目操作
 **/
public interface BinaryOperator extends Serializable {

  Layer run(Feature f, Layer layer);

  Layer run(Geometry f, Layer layer);

  Layer run(Geometry f, IndexedLayer layer);

  Layer run(Feature f, IndexedLayer layer);

  Layer run(Layer layer1, IndexedLayer layer2);

  Layer run(IndexedLayer layer1, IndexedLayer layer2);

  Layer run(List<Feature> features, Layer layer);

  Layer run(List<Feature> features, IndexedLayer layer);

}
