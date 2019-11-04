package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import lombok.Getter;

/**
 * @author Hu
 * @date 2019/9/20
 * 支持的数据源类型
 **/
public enum SourceType {

  FILE(0), MONGODB(1);

  @Getter
  private int type;

  SourceType(int type) {
    this.type = type;
  }

}
