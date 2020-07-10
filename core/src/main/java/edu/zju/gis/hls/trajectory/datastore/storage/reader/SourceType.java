package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Hu
 * @date 2019/9/20
 * 支持的数据源类型
 **/
@Slf4j
public enum SourceType {

  FILE(0, "file://"), MONGODB(1, "mongo://"), SHP(2, "shp://"), ES(3, "es://"),
  HDFS_FILE(4, "hdfs://"), HDFS_SHP(5, "hdfshp://"), PG(6, "pg://"),
  MYSQL(7, "mysql://"), PLATFORM(8, "platform://");

  @Getter
  private int type;

  private String prefix;

  SourceType(int type, String prefix) {
    this.type = type;
    this.prefix = prefix;
  }

  public static SourceType getSourceType(String path) {
    for (SourceType st: SourceType.values()) {
      if (path.startsWith(st.prefix)) return st;
    }
    throw new GISSparkException("Unsupport source type: " + path);
  }

}
