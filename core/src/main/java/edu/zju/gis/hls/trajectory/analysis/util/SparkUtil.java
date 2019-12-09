package edu.zju.gis.hls.trajectory.analysis.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author Hu
 * @date 2019/11/8
 **/
public class SparkUtil {

  public static SparkSession createLocalSparkSession(String appName, SparkConf conf) {
    SparkSession ss = SparkSession
      .builder()
      .appName(appName)
      .master("local[4]")
      .config(conf)
      .getOrCreate();
    return ss;
  }

  public static SparkSession createRemoteSparkSession(String appName, SparkConf conf) {
    SparkSession ss = SparkSession
      .builder()
      .appName(appName)
      .master("spark://192.168.1.5:7077")
      .config(conf)
      .getOrCreate();
    return ss;
  }

  public static SparkSession createSparkSession(String appName, SparkConf conf) {
    SparkSession ss = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .getOrCreate();
    return ss;
  }

}
