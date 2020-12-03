package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.datastore.storage.writer.ImageWriterConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-09-16
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
