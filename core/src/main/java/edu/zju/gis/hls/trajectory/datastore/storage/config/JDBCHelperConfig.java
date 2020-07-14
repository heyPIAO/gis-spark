package edu.zju.gis.hls.trajectory.datastore.storage.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Hu
 * @date 2020/7/14
 **/
@Getter
@Setter
@ToString(callSuper = true)
public abstract class JDBCHelperConfig extends DataSourceConfig {

  protected String url;
  protected int port;
  protected String username;
  protected String password;
  protected String database;

  public JDBCHelperConfig(String url, int port, String username, String password, String database) {
    this.url = initUrl(url);
    this.port = port;
    this.username = username;
    this.password = password;
    this.database = database;
  }

//  public JDBCHelperConfig() {
//    this("localhost", 3306, "root", "root", "test");
//  }

  /**
   * 覆盖一些必要参数
   * @param url
   * @return
   */
  protected String initUrl(String url) {
    StringBuilder sb = new StringBuilder(url);
    if (url.lastIndexOf("?") == -1) {
      sb.append("?");
    } else {
      sb.append("&");
    }
    sb.append("rewriteBatchedStatements=true");
    return sb.toString();
  }

  protected abstract String getDriver();

}
