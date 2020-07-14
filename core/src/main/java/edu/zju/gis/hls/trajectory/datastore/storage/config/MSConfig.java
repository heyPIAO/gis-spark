package edu.zju.gis.hls.trajectory.datastore.storage.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * MySQL 配置类
 * @author Hu
 * @date 2020/7/14
 **/
@Getter
@Setter
@ToString(callSuper = true)
public class MSConfig extends JDBCHelperConfig {

  public MSConfig() {
    this("localhost", 3306, "root", "root", "mysql");
  }

  public MSConfig(String url, int port, String username, String password, String database) {
    super(url, port, username, password, database);
  }

  @Override
  protected String getDriver() {
    return "com.mysql.cj.jdbc.Driver";
  }

}
