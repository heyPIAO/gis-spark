package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Hu
 * @date 2019/9/21
 **/
@Getter
@Setter
@ToString
public class LayerMetadata implements Serializable {
  public String layerId;
  public String layerName;
  public double[] extent;

  public LayerMetadata() {
    this.layerId = UUID.randomUUID().toString();
    this.layerName = this.layerId;
    this.extent = new double[4];
  }

  public LayerMetadata(LayerMetadata metadata) {
    this.layerId = metadata.layerId;
    this.layerName = metadata.layerName;
    this.extent = metadata.extent;
  }
}
