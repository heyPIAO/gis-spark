package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.util.FileUtil;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.ReaderConfigTerm;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.mongo.MongoLayerWriter;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
    if (this.writerConfig.isKeepKey()) {

      layer.makeSureCached();
      int partitionNum = layer.distinctKeys().size();
      JavaPairRDD<String, String> t = layer.mapToPair(x-> {
        Tuple2<String, Feature> m = (Tuple2<String, Feature>) x;
        return new Tuple2<>(m._1, m._2.toString());
      }).partitionBy(new HashPartitioner(partitionNum));

//      JavaRDD<String> re = t.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, String>>, String>() {
//        @Override
//        public Iterator<String> call(Iterator<Tuple2<String, String>> iter) throws Exception {
//
//          // t.saveAsTextFile(outDir);
//          // TODO 这种重写方式有点蠢，搞清楚 saveAsHadoopFile 原理后还是要用 FileOutputFormat 重写
//          List<String> result = new ArrayList<>();
//
//          if (iter.hasNext()) {
//
//            Tuple2<String, String> m = iter.next();
//            String gridId = m._1;
//            String value = gridId + ReaderConfigTerm.DEFAULT_FILE_SEPARATOR + m._2 + "\n";
//
//            result.add("success: " + gridId);
//
//            // 创建输出文件
//            File file = new File(writerConfig.getSinkPath() + File.separator + gridId);
////            if (file.exists()) {
////              file.delete();
////            }
//            file.createNewFile();
//            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
//            bos.write(value.getBytes());
//
//            while (iter.hasNext()) {
//              m = iter.next();
//              bos.write((m._1 + ReaderConfigTerm.DEFAULT_FILE_SEPARATOR + m._2 + "\n").getBytes());
//            }
//
//            bos.flush();
//            bos.close();
//          }
//
//          return result.iterator();
//        }
//      });
//
//      List<String> r = re.collect();

      t.saveAsHadoopFile(writerConfig.getSinkPath(), String.class, String.class, KeyFileOutputFormat.class);
      logger.info("Write layer to directory " + writerConfig.getSinkPath() + " with partition key as file name successfully");
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
      outputData.saveAsTextFile(writerConfig.getSinkPath());
      logger.info("Write layer to directory " + writerConfig.getSinkPath() + " successfully");
    }
  }

  public void write(KeyIndexedLayer layer) {
    this.write(layer.toLayer());
  }

}
