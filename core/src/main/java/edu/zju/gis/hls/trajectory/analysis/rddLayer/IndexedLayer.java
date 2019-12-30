package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.datastore.exception.DataQueryException;
import lombok.Getter;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/12/30
 **/
public abstract class IndexedLayer<L extends Layer> implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(IndexedLayer.class);

  @Getter
  protected IndexType indexType;

  /**
   * 获取指定空间对象范围内的对象
   * @param geometry
   * @return
   */
  public abstract IndexedLayer<L> query(Geometry geometry);

  /**
   * 获取指定图层范围内的对象
   * @param layer
   * @return
   */
  public IndexedLayer<L> query(Layer layer) {
    if (layer.getMetadata().getExtent() == null) {
      logger.error("layer extent has not be calculated, please run layer.analyze() first");
      throw new DataQueryException("layer extent has not be calculated, please run layer.analyze() first");
    }
    return this.query(layer.getMetadata().getGeometry());
  }

  public abstract L toLayer();

}
