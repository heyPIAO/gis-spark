package edu.zju.gis.hls.trajectory.datastore.storage.reader.shp;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author Hu
 * @date 2020/9/14
 **/
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MdbLayerReaderConfig extends ShpLayerReaderConfig {
  private String targetLayerName;
}
