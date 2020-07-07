package edu.zju.gis.hls.trajectory.datastore.storage.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2020/7/6
 * TODO 参数有点多，建议改成builder模式，或者用option配置
 **/
@Getter
@Setter
@ToString
@AllArgsConstructor
public class PgConfig implements DataSourceConfig, Serializable {

  private String url;
  private int port;
  private String username;
  private String password;
  private String database;
  private String schema;

  public PgConfig() {
    this("localhost", 5432, "postgres", "postgres", "postgres", "public");
  }

}
