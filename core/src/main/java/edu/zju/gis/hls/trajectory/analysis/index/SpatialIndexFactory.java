package edu.zju.gis.hls.trajectory.analysis.index;

import edu.zju.gis.hls.trajectory.analysis.index.quadtree.QuadTreeIndex;
import edu.zju.gis.hls.trajectory.analysis.index.quadtree.QuadTreeIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.index.rtree.RTreeIndex;
import edu.zju.gis.hls.trajectory.analysis.index.rtree.RTreeIndexConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Hu
 * @date 2019/12/16
 **/
@Slf4j
public class SpatialIndexFactory {

  public static DistributeSpatialIndex getDistributedSpatialIndex(IndexType type) {
    return SpatialIndexFactory.getDistributedSpatialIndex(type, null);
  }

  public static InnerSpatialIndex getInnerSpatialIndex(IndexType type) {
    return SpatialIndexFactory.getInnerSpatialIndex(type, null);
  }

  public static DistributeSpatialIndex getDistributedSpatialIndex(IndexType type, IndexConfig config) {
    switch (type) {
      case QUADTREE: return config == null ? new QuadTreeIndex():new QuadTreeIndex((QuadTreeIndexConfig) config);
      default:
        log.error("Unvalid distributed spatial index type");
        throw new UnsupportedOperationException("Unvalid distributed spatial index type");
    }
  }

  public static InnerSpatialIndex getInnerSpatialIndex(IndexType type, IndexConfig config) {
    switch (type) {
      case RTREE: return config == null ? new RTreeIndex():new RTreeIndex((RTreeIndexConfig) config);
      default:
        log.error("Unvalid inner spatial index type");
        throw new UnsupportedOperationException("Unvalid inner spatial index type");
    }
  }

}
