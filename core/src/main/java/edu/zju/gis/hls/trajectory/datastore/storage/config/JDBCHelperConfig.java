package edu.zju.gis.hls.trajectory.datastore.storage.config;

import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public abstract class JDBCHelperConfig extends DataSourceConfig {

  protected String url;
  protected int port;
  protected String username;
  protected String password;
  protected String database;

  protected abstract String getDriver();

}
