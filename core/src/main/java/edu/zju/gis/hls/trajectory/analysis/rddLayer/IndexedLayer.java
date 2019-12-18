package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Hu
 * @date 2019/12/16
 * 已经构建过索引的图层
 **/
@Getter
@Setter
public abstract class IndexedLayer <K,V extends Feature> {

  private static final Logger logger = LoggerFactory.getLogger(IndexedLayer.class);

  private IndexType indexType;

  private Layer<K, V> layer;

  /**
   * 获取指定空间对象范围内的对象
   * @param geometry
   * @return
   */
  public abstract IndexedLayer query(Geometry geometry);

  /**
   * 获取指定图层范围内的对象
   * @param layer
   * @return
   */
  public IndexedLayer query(Layer layer) {
    return this.query(layer.getMetadata().getGeometry());
  }

  public IndexedLayer copy(IndexedLayer from, IndexedLayer to) {
    to.indexType = from.indexType;
    return to;
  }

  public LayerMetadata getMetadata() {
    return this.layer.metadata;
  }

  public Map<Integer, Field> getFields() {
    return this.layer.fields;
  }

}
