package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

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
}
