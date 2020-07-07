package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import edu.zju.gis.hls.trajectory.datastore.storage.config.PgConfig;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Hu
 * @date 2020/7/6
 * PostgreSQL 常用操作类
 **/
public class PgHelper implements StorageHelper {

  private static final Logger logger = LoggerFactory.getLogger(PgHelper.class);

  private static PgHelper instance = null;

  @Getter
  private PgConfig config;

  private PgHelper(PgConfig config) {
    this.config = config;
  }

  private PgHelper() {
    this.config = new PgConfig();
  }

  public static PgHelper getHelper() {
    return getHelper(null);
  }

  public static PgHelper getHelper(PgConfig config) {
    if (instance == null) {
      if (config == null) {
        logger.warn("MongoHelper has already inited, configuration abort");
      }
    } else {
      if (config == null) {
        instance = new PgHelper();
      } else {
        instance = new PgHelper(config);
      }
    }
    return instance;
  }

  @Override
  public void useDB(String databaseName) {

  }

  @Override
  public long insert(String tableName, Map<String, ?> data) {
    return 0;
  }

  @Override
  public long insert(String tableName, List<Map<String, ?>> data) {
    return 0;
  }

  @Override
  public long getSize(String tableName) {
    return 0;
  }

  @Override
  public void initReader(String tableName) {

  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public String next() {
    return null;
  }

  @Override
  public void closeReader() {

  }
}
