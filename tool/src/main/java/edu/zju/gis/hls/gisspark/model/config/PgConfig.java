package edu.zju.gis.hls.gisspark.model.config;

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
    this("localhost", 5432, "postgres", "root", "postgres", "public");
  }

  public class PgConfigBuilder {

    private PgConfig config;

    public PgConfigBuilder() {
      this.config = new PgConfig();
    }

    public PgConfig build() {
      return this.config;
    }

    public void setPort(int port) {
      this.config.setPort(port);
    }

    public void setUrl(String url) {
      this.config.setUrl(url);
    }

    public void setUsername(String username) {
      this.config.setUsername(username);
    }

    public void setPassword(String password) {
      this.config.setUsername(password);
    }

    public void setDatabase(String database) {
      this.config.setUsername(database);
    }

    public void setSchema(String schema) {
      this.config.setUsername(schema);
    }

  }

}
