package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataType;

/**
 * @author Hu
 * @date 2019/9/20
 * 支持的数据源类型
 **/
@Slf4j
public enum SourceType {

  FILE(0), MONGODB(1), SHP(2), ES(3), HDFS_FILE(4), HDFS_SHP(5), PG(6), MYSQL(7), PLATFORM(8);

  @Getter
  private int type;

  SourceType(int type) {
    this.type = type;
  }

}
