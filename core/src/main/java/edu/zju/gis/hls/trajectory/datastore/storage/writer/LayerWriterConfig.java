package edu.zju.gis.hls.trajectory.datastore.storage.writer;

import lombok.*;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/12/19
 **/
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public abstract class LayerWriterConfig implements Serializable {
  protected String sinkPath;
}
