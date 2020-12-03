package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/18
 * 每一个子类都必须实现单例
 **/
public interface JDBCHelper extends Closeable {

  void useDB(String databaseName);

  long insert(String tableName, Map<String, Object> data);

  long insert(String tableName, List<Map<String, Object>> data);

  long getSize(String tableName);

  /**
   * 打开数据 reader
   * @param tableName
   */
  void initReader(String tableName);

  /**
   * 是否还有值
   * @return
   */
   boolean hasNext();

  /**
   * 返回一个JsonString
   * @return
   */
  String next();

  /**
   * 关闭 reader
   */
  void closeReader();

  <T> void runSQL(String sql, SQLResultHandler<T> callBack);

  boolean runSQL(String sql);

}
