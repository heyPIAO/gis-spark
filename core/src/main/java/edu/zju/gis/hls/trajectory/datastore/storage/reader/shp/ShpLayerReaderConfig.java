package edu.zju.gis.hls.trajectory.datastore.storage.reader.shp;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2019/12/19
 **/
public class ShpLayerReaderConfig extends LayerReaderConfig {

  @Getter
  @Setter
  private String encodeType = "UTF8";

  public ShpLayerReaderConfig(String layerName, String sourcePath, LayerType layerType, String encodeType) {
    super(layerName, sourcePath, layerType);
    this.encodeType = encodeType;
  }

  public ShpLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    super(layerName, sourcePath, layerType);
  }

}
