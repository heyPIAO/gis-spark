package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.datastore.exception.DataQueryException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.Function;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

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
  public abstract <I extends IndexedLayer<L>> I query(Geometry geometry);

  public <I extends IndexedLayer<L>> I query(Feature feature) {
    return this.query(feature.getGeometry());
  }

  /**
   * 获取指定空间对象范围内的对象
   * @param geometries
   * @return
   * TODO It's dangerous to return null, modify it by return an empty layer
   */
  public <I extends IndexedLayer<L>> I query(List<Geometry> geometries) {
    if (geometries.size() == 0) {
      log.warn("empty geometry list for indexed layer query, return null");
      return null;
    }
    Geometry g = geometries.get(0);
    for (int i=1; i<geometries.size(); i++) {
      g = g.union(geometries.get(1));
    }
    return this.query(g);
  }

  // TODO 通过 Function 支持对于与某一个geometry intersect的图斑的进一步操作
  public <I extends IndexedLayer<L>> I query(List<Geometry> geometries, BiFunction<Feature, Geometry, Feature> f) {
    if (geometries.size() == 0) {
      log.warn("empty geometry list for indexed layer query, return null");
      return null;
    }
    Geometry g = geometries.get(0);
    for (int i=1; i<geometries.size(); i++) {
      g = g.union(geometries.get(1));
    }
    return this.query(g);
  }

  /**
   * 获取指定图层四至范围内的对象
   * @param layer
   * @return
   */
  public <I extends IndexedLayer<L>> I query(Layer layer) {
    if (layer.getMetadata().getExtent() == null) {
      log.error("layer extent has not be calculated, please run layer.analyze() first");
      throw new DataQueryException("layer extent has not be calculated, please run layer.analyze() first");
    }
    return this.query(layer.getMetadata().getGeometry());
  }

  public abstract L toLayer();

}
