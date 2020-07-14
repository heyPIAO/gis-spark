package edu.zju.gis.hls.trajectory.datastore.storage.reader.platform;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.NoArgsConstructor;

/**
 * @author Hu
 * @date 2020/7/3
 **/
@NoArgsConstructor
public class PlatformLayerReaderConfig extends LayerReaderConfig  {

  public PlatformLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    super(layerName, sourcePath, layerType);
  }

}
