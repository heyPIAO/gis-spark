package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.exception.WriterException;
import edu.zju.gis.hls.trajectory.datastore.storage.helper.file.HDFSHelper;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.mongo.MongoLayerWriter;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author Hu
 * @date 2019/11/8
 * 将图层写出到文件
 **/
public class FileLayerWriter extends LayerWriter<String> {

  private static final Logger logger = LoggerFactory.getLogger(MongoLayerWriter.class);

  @Getter
  @Setter
  private FileLayerWriterConfig writerConfig;

  public FileLayerWriter(SparkSession ss, FileLayerWriterConfig writerConfig) {
    super(ss);
    this.writerConfig = writerConfig;
  }

  @Override
  public String transform(Feature feature) {
    return feature.toString();
  }

  /**
   * 写成普通的 wkt 文件，每个 partition 一个文件
   * TODO 写出成 Parquet 文件
   * TODO 从 hdfs 环境中读取默认输出路径
   * TODO 缓存清理机制需要再考虑，建议在构建indexLayer时就通过repartition完成shuffle
   * @param layer
   */
  @Override
  public void write(Layer layer) {
    String outDir = this.writerConfig.getSinkPath();
    if (this.writerConfig.isKeepKey()) {
      layer.cache();
      int partitionNum = layer.distinctKeys().size();
      JavaPairRDD<String, String> t = layer.mapToPair(x-> {
        Tuple2<String, Feature> m = (Tuple2<String, Feature>) x;
        return new Tuple2<>(m._1, m._2.toString());
      }).repartition(partitionNum);
      t.saveAsHadoopFile(outDir, String.class, String.class, KeyFileOutputFormat.class);
      layer.unpersist();
      logger.info("Write layer to directory " + outDir + " with partition key as file name successfully");
    } else {
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
      logger.info("Write layer to directory " + outDir + " successfully");
    }
  }

  public void write(KeyIndexedLayer layer) {
    this.write(layer.toLayer());
  }

}
