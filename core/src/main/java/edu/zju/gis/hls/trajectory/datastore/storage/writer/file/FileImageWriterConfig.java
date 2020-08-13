package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.datastore.storage.writer.ImageWriterConfig;
import lombok.*;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-08-13
 */
@Getter
@Setter
public class FileImageWriterConfig extends ImageWriterConfig {
  public FileImageWriterConfig() {
    super();
  }

  public FileImageWriterConfig(String dir) {
    super(dir);
  }
}
