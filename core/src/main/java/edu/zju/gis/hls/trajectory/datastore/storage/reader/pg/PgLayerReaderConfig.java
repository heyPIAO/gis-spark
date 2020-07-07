package edu.zju.gis.hls.trajectory.datastore.storage.reader.pg;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Hu
 * @date 2020/7/1
 * sourcePath === url "jdbc:postgresql:dbserver"
 **/
@Getter
@Setter
@ToString
public class PgLayerReaderConfig extends LayerReaderConfig {

  private String schema;
  private String dbtable;
  private String username;
  private String password;

  @Override
  public boolean check() {
    return super.check() &&(schema !=null && schema.trim().length() > 0)
      && (dbtable !=null && dbtable.trim().length() > 0)
      && (username != null && username.trim().length() > 0)
      && (password != null && password.trim().length() > 0);
  }

  public PgLayerReaderConfig(String layerName, String sourcePath, LayerType layerType) {
    super(layerName, sourcePath, layerType);
  }

}
