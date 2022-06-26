package edu.zju.gis.hls.trajectory.datastore.storage.writer.pg;

import edu.zju.gis.hls.trajectory.datastore.datasource.params.JdbcParams;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.spark.sql.SaveMode;

/**
 * @author Hu
 * @date 2020/7/1
 * 将图层数据写出到 Postgis 数据库配置
 **/
@Getter
@Setter
@ToString
@NoArgsConstructor
public class PgLayerWriterConfig extends LayerWriterConfig {

  private final String driver = "org.postgresql.Driver";

  private String schema;
  private String tablename;
  private String username;
  private String password;
  private SaveMode saveMode = SaveMode.Append;

  public PgLayerWriterConfig(String tablename) {
    this(tablename, "postgres", "postgres");
  }

  public PgLayerWriterConfig(String tablename, String username, String password) {
    this("public", tablename, username, password);
  }

  public PgLayerWriterConfig(String schema, String tablename, String username, String password) {
    this("jdbc:postgresql://localhost:5432/postgres", schema, tablename, username, password);
  }

  public PgLayerWriterConfig(String sinkPath, String schema, String tablename, String username, String password) {
    super(sinkPath);
    this.schema = schema;
    this.tablename = tablename;
    this.username = username;
    this.password = password;
  }

  public JdbcParams toJdbcParams(){
    return new JdbcParams().setTableName(this.getTablename())
            .setJdbcDriver(this.getDriver())
            .setJdbcUrl(this.getSinkPath())
            .setPassword(this.getPassword())
            .setUserName(this.getUsername());
  }
}
