package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Hu
 * @date 2019/12/30
 * 分区内部二层索引的图层
 **/
@Slf4j
public abstract class PartitionIndexedLayer<L extends Layer, K extends KeyIndexedLayer<L>> extends IndexedLayer<L> {

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
