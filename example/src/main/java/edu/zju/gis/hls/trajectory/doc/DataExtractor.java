package edu.zju.gis.hls.trajectory.doc;

import edu.zju.gis.hls.trajectory.analysis.util.DateUtils;
import edu.zju.gis.hls.trajectory.analysis.util.FileUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @author Hu
 * @date 2019/9/9
 * 从 orc 文件中过滤目标数据
 **/
public class DataExtractor {

  public static void main(String[] args) throws IOException {
    // getTimeRange();
    extractData(args[0], args[1]);
  }

  public static void getTimeRange() {
    String time = "1502542657976";
    Date date = DateUtils.getDateMS(time);
    System.out.println(date);
    // 获取一天的开始和结束
    Date start = DateUtils.beginOfDay(date);
    Date end = DateUtils.beginOfDay(DateUtils.addDays(date, 1));
    System.out.println(start);
    // 1502553600000
    System.out.println("start time millsecond: " + start.getTime());
    System.out.println(end);
    // 1502640000000
    System.out.println("end time millsecond: " + end.getTime());
  }

  public static void getMinAndMaxTimeStamp(String dirpath) {

    // Setup environment
    SparkConf conf = new SparkConf();
    conf.setAppName("time_range");
    conf.setMaster("localhost[*]");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // Read all data
    JavaRDD<String> data = jsc.textFile("file://" + dirpath + "/*/*");
    JavaRDD<Long> timestamp = data.map(new Function<String, Long>() {
      public Long call(String s) throws Exception {
        String time = s.split(",")[1];
        return Long.valueOf(time);
      }
    });
    timestamp.persist(StorageLevel.MEMORY_ONLY());
    Long minRdd = timestamp.reduce(new Function2<Long, Long, Long>() {
      public Long call(Long aLong, Long aLong2) throws Exception {
        return aLong<aLong2 ? aLong:aLong2;
      }
    });

    Long maxRdd = timestamp.reduce(new Function2<Long, Long, Long>() {
      public Long call(Long aLong, Long aLong2) throws Exception {
        return aLong>aLong2 ? aLong:aLong2;
      }
    });

    System.out.println("max = " + maxRdd);
    System.out.println("min = " + minRdd);
  }

  public static void extractData(final String dirpath, String outputPath) throws IOException {
    // Setup environment
    SparkConf conf = new SparkConf();
    conf.setAppName("time_range");
    conf.setMaster("yarn-cluster");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // Read all data
    JavaRDD<String> data = jsc.textFile("file://" + dirpath + "/*/*");
    JavaRDD<String> timestamp = data.mapToPair(new PairFunction<String, String, String>() {
      public Tuple2<String, String> call(String s) throws Exception {
        String[] fields = s.split(",");
        return new Tuple2<String, String>(fields[fields.length-1], s);
      }
    }).groupByKey().map(new Function<Tuple2<String, Iterable<String>>, String>() {
      public String call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
        FileUtil.write(dirpath + "/" + stringIterableTuple2._1 + ".txt", stringIterableTuple2._2);
        return stringIterableTuple2._1;
      }
    });

    List<String> result = timestamp.collect();
    result.forEach(System.out::println);
  }

}
