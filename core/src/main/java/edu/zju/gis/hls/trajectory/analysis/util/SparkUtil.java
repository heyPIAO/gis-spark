package edu.zju.gis.hls.trajectory.analysis.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author Hu
 * @date 2019/11/8
 **/
public class SparkUtil {

  private static SparkConf coverSC(SparkConf conf) {
    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"); // 不输出 _success
    return conf;
  }

  public static SparkSession createLocalSparkSession(String appName, SparkConf conf) {
    SparkSession ss = SparkSession
      .builder()
      .appName(appName)
      .master("local[4]")
      .config(coverSC(conf))
      .getOrCreate();
    return ss;
  }

  public static SparkSession createRemoteSparkSession(String appName, SparkConf conf) {
    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
    SparkSession ss = SparkSession
      .builder()
      .appName(appName)
      .master("spark://192.168.1.5:7077")
      .config(coverSC(conf))
      .getOrCreate();
    return ss;
  }

  public static SparkSession createSparkSession(String appName, SparkConf conf) {
    conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");
    SparkSession ss = SparkSession
      .builder()
      .appName(appName)
      .config(coverSC(conf))
      .getOrCreate();
    return ss;
  }

}
