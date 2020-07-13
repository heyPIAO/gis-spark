package edu.zju.gis.hls.trajectory.datastore.storage.reader.file;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.ReaderConfigTerm;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Hu
 * @date 2019/12/19
 * -1 means the last column, -99 means the first column;
 **/
@Getter
@Setter
@ToString(callSuper = true)
@NoArgsConstructor
public class FileLayerReaderConfig extends LayerReaderConfig {

  private String separator = ReaderConfigTerm.DEFAULT_FILE_SEPARATOR;

  public FileLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    super(layerName, sourcePath, layerType);
  }

}
