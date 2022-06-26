package edu.zju.gis.hls.gisspark.model.util;

import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author Hu
 * @date 2019/11/8
 **/
public class SparkUtil {

    public static SparkSession getSparkSession(SparkSessionType type, String appName) {
        return getSparkSession(type, appName, new SparkConf());
    }

    public static SparkSession getSparkSession(SparkSessionType type, String appName, SparkConf conf) {
        switch (type) {
            case LOCAL:
                return createLocalSparkSession(appName, conf);
            case REMOTE:
                return createRemoteSparkSession(appName, conf);
            case SHELL:
                return createShellSparkSession(appName, conf);
            default:
                throw new GISSparkException("Unsupport Spark Session Type: " + type.name());
        }
    }

    //TODO put these configuration into property file
    private static SparkConf overrideConf(SparkConf conf) {
      conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"); // 不输出 _success
//    conf.set("es.index.auto.create", "true");
//    conf.set("es.nodes", "121.89.211.142");
//    conf.set("es.nodes.discovery", "true");
//    conf.set("es.port", "9200");
//    conf.set("es.mapping.date.rich", "false");
//    conf.set("es.nodes.wan.only","true");
        return conf;
    }

    private static SparkSession createLocalSparkSession(String appName, SparkConf conf) {
        //add .enableHiveSupport() for hive
        return SparkSession
                .builder()
                .appName(appName)
                .master("local[4]")
                .config(conf)
                .config("spark.driver.host", "localhost")
//                .config(overrideConf(conf))
//                .enableHiveSupport()
                .getOrCreate();
    }

    // TODO put remote spark url to property file
    private static SparkSession createRemoteSparkSession(String appName, SparkConf conf) {
        return SparkSession
                .builder()
                .appName(appName)
//      .master("spark://192.168.1.5:7077")
                .config(overrideConf(conf))
                .getOrCreate();
    }

    private static SparkSession createShellSparkSession(String appName, SparkConf conf) {
        return SparkSession
                .builder()
//      .appName(appName).master("yarn")
                .config(overrideConf(conf))
                .getOrCreate();
    }

}
