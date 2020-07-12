package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Hu
 * @date 2019/12/19
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class FileLayerWriterConfig extends LayerWriterConfig {

  private boolean keepKey; // 是否保留 KeyedIndexLayer 的分区key 作为文件名

  public FileLayerWriterConfig() {
    this(null);
  }

  public FileLayerWriterConfig(String sinkPath) {
    this(sinkPath, false);
  }

  public FileLayerWriterConfig(String sinkPath, boolean keepKey) {
    super(sinkPath);
    this.keepKey = keepKey;
  }

}
