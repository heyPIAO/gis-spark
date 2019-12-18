package edu.zju.gis.hls.trajectory.analysis.index;

import edu.zju.gis.hls.trajectory.analysis.index.quadtree.QuadTreeIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hu
 * @date 2019/12/16
 **/
public class SpatialIndexFactory {

  private static final Logger logger = LoggerFactory.getLogger(SpatialIndexFactory.class);

  public static SpatialIndex getSpatialIndex(IndexType type) {
    switch (type) {
      case QUADTREE: return new QuadTreeIndex();
      default:
        logger.error("Unvalid spatial index type");
        throw new UnsupportedOperationException("Unvalid spatial index type");
    }
  }

}
