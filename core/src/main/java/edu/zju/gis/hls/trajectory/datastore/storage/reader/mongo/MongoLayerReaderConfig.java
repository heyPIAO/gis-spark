package edu.zju.gis.hls.trajectory.datastore.storage.reader.mongo;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.NoArgsConstructor;

/**
 * @author Hu
 * @date 2019/12/19
 **/
@NoArgsConstructor
public class MongoLayerReaderConfig extends LayerReaderConfig {

  public MongoLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    super(layerName, sourcePath, layerType);
  }

}
