package edu.zju.gis.hls.trajectory.datastore.storage.writer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * @author Hu
 * @date 2019/11/8
 **/
public class FileLayerWriter extends LayerWriter <String> {

  private static final Logger logger = LoggerFactory.getLogger(MongoDBLayerWriter.class);

  public FileLayerWriter(SparkSession ss) {
    super(ss);
  }

  @Override
  public String transform(Feature feature) {
    return feature.toString();
  }

  /**
   * 写成普通的 wkt 文件
   * TODO 写出成 Parquet 文件
   * TODO 从 hdfs 环境中读取默认输出路径
   * @param layer
   * @param prop
   */
  @Override
  public void write(Layer layer, Properties prop) {
    String outDir = prop.getProperty("uri");
    JavaRDD<Tuple2<String, Feature>> t = layer.rdd().toJavaRDD();
    JavaRDD<String> outputData = t.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Feature>>, String>() {
      @Override
      public Iterator<String> call(Iterator<Tuple2<String, Feature>> in) throws Exception {
        List<String> result = new ArrayList<>();
        while(in.hasNext()) {
          result.add(transform(in.next()._2));
        }
        return result.iterator();
      }
    });
    outputData.saveAsTextFile(outDir);
  }

}
