package edu.zju.gis.hls.trajectory.datastore.storage.reader.pg;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
@NoArgsConstructor
public class PgLayerReaderConfig extends LayerReaderConfig {

  private String schema;
  private String dbtable;
  private String username;
  private String password;
  private String filter = "1=1"; // 过滤条件

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

  // TODO 改成用正则取出来
  public String getUrl() {
    return this.sourcePath.split(":")[2].replace("//", "");
  }

  // TODO 改成用正则取出来
  public int getPort() {
    return Integer.valueOf(this.sourcePath.split(":")[3].split("/")[0]);
  }

  // TODO 改成用正则取出来
  public String getDatabase() {
    return this.sourcePath.split(":")[3].split("/")[1];
  }

  public String getFilterSql(String tablename) {
    return String.format("select * from %s where %s", tablename, this.filter);
  }

  public String getFilterSql() {
    return String.format("select * from %s.%s where %s", this.schema, this.dbtable, this.filter);
  }

}
