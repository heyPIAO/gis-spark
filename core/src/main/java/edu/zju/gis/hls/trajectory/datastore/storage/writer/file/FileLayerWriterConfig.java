package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Hu
 * @date 2019/12/19
 **/
@Getter
@Setter
public class FileLayerWriterConfig extends LayerWriterConfig {
  private boolean keepKey; // 是否保留 KeyedIndexLayer 的分区key 作为文件名
}
