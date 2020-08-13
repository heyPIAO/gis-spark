package edu.zju.gis.hls.trajectory.datastore.storage.writer;

import lombok.*;

import java.io.Serializable;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-08-13
 */
@Getter
@Setter
public abstract class ImageWriterConfig implements Serializable {
  protected String baseDir;

  protected ImageWriterConfig() {
    this("/");
  }

  protected ImageWriterConfig(String dir) {
    this.baseDir = dir;
  }
}
