package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import edu.zju.gis.hls.trajectory.datastore.storage.config.PgConfig;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Hu
 * @date 2020/7/6
 * PostgreSQL 常用操作类
 * TODO 待测
 **/
@Slf4j
@ToString(callSuper = true)
public class PgHelper extends JDBCHelperImpl<PgConfig> {

  private static PgHelper instance = null;

  private PgHelper(PgConfig config) {
    super(config);
  }

  private PgHelper() {
    this(new PgConfig());
  }

  public static PgHelper getHelper() {
    return PgHelper.getHelper(new PgConfig());
  }

  public static PgHelper getHelper(PgConfig config) {
    if (instance != null) {
      log.warn("PgHelper has already inited, configuration abort");
    } else {
      instance = new PgHelper(config);
    }
    return instance;
  }

  @Override
  public String transformTableName(String tableName) {
    return String.format("%s.\"%s\"", this.config.getSchema(), tableName);
  }
}
