package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.datastore.exception.DataQueryException;
import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hu
 * @date 2019/12/16
 * 基于Key构建索引
 **/
public abstract class KeyIndexedLayer<L extends Layer> extends IndexedLayer<L> {

  private static final Logger logger = LoggerFactory.getLogger(KeyIndexedLayer.class);

  @Getter
  @Setter
  protected L layer;

  public KeyIndexedLayer<L> copy(KeyIndexedLayer<L> from, KeyIndexedLayer<L> to) {
    to.indexType = from.indexType;
    return to;
  }

  public LayerMetadata getMetadata() {
    return this.layer.metadata;
  }

  public Field[] getAttributes() {
    return this.layer.metadata.getAttributes().keySet().toArray(new Field[]{});
  }

  @Override
  public L toLayer() {
    return this.layer;
  }

}
