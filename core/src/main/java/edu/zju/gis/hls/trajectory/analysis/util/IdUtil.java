package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;

/**
 * @author Hu
 * @date 2020/9/16
 **/
public class IdUtil {

  /**
   * TODO 利用 Twitter 雪花算法实现全局唯一 64bit long 型ID
   * 算法中引入时间戳，基本可以保持按时间自增
   * @return
   */
  public static long generate(long workerId, long datacenterId, long sequence) {
    throw new GISSparkException("Under Construction");
  }

}
