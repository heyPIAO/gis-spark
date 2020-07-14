package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import edu.zju.gis.hls.trajectory.datastore.storage.config.MSConfig;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * MySQL 常用操作类
 * @author Hu
 * @date 2020/7/14
 * TODO 待测
 **/
@Slf4j
@ToString(callSuper = true)
public class MSHelper extends JDBCHelperImpl<MSConfig> {

  private static MSHelper instance = null;

  public MSHelper(MSConfig config) {
    super(config);
  }

  private MSHelper() {
    this(new MSConfig());
  }

  public static MSHelper getHelper() {
    return MSHelper.getHelper(new MSConfig());
  }

  public static MSHelper getHelper(MSConfig config) {
    if (instance != null) {
      log.warn("MSHelper has already inited, configuration abort");
    } else {
      instance = new MSHelper(config);
    }
    return instance;
  }

  @Override
  public String transformTableName(String tableName) {
    return tableName;
  }

}
