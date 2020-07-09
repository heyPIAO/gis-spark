package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.datastore.exception.DataQueryException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/12/30
 **/
@Slf4j
public abstract class IndexedLayer<L extends Layer> implements Serializable {

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
      log.error("layer extent has not be calculated, please run layer.analyze() first");
      throw new DataQueryException("layer extent has not be calculated, please run layer.analyze() first");
    }
    return this.query(layer.getMetadata().getGeometry());
  }

  public abstract L toLayer();

}
