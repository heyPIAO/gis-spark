package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hu
 * @date 2019/12/18
 **/
public class QuadTreeIndexLayer<K,V extends Feature> extends IndexedLayer<K,V> {

  private static final Logger logger = LoggerFactory.getLogger(QuadTreeIndexLayer.class);

  @Override
  public IndexedLayer query(Geometry geometry) {
    return null;
  }

}
