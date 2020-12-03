package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


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

  public LayerMetadata getMetadata() {
    return this.layer.getMetadata();
  }

  public Field[] getAttributes() {
    return this.layer.getAttributes();
  }

  @Override
  public L toLayer() {
    return this.layer.toLayer();
  }

}
