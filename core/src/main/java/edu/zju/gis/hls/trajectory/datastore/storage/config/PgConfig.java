package edu.zju.gis.hls.trajectory.datastore.storage.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


/**
 * @author Hu
 * @date 2020/7/6
 * TODO 参数有点多，建议改成builder模式，或者用option配置
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class PgConfig extends JDBCHelperConfig {

  private String schema;

  public PgConfig(String url, int port, String username, String password, String database, String schema) {
    super(url, port, username, password, database);
    this.schema = schema;
  }

  public PgConfig() {
    this("localhost", 5432, "postgres", "root", "postgres", "public");
  }

  @Override
  protected String getDriver() {
    return "org.postgresql.Driver";
  }
}
