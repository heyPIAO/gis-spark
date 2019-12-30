package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import lombok.Getter;
import lombok.Setter;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Hu
 * @date 2019/12/30
 **/
public abstract class PartitionIndexedLayer<L extends Layer, K extends KeyIndexedLayer<L>> extends IndexedLayer<L> {

  private static final Logger logger = LoggerFactory.getLogger(PartitionIndexedLayer.class);

  @Getter
  @Setter
  protected K layer;

  public abstract PartitionIndexedLayer<L, K> query(Geometry geometry);

  public LayerMetadata getMetadata() {
    return this.layer.getLayer().metadata;
  }

  public Field[] getAttributes() {
    return this.layer.getLayer().metadata.getAttributes().keySet().toArray(new Field[]{});
  }

  @Override
  public L toLayer() {
    return this.layer.toLayer();
  }

}
