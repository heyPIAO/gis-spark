package edu.zju.gis.hls.trajectory.datastore.storage.writer.mongo;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/20
 * 将图层数据写到 MongoDB
 * 默认新建表。如果表已经存在，则构建失败
 **/
public class MongoLayerWriter extends LayerWriter<Document> {

  private static final Logger logger = LoggerFactory.getLogger(MongoLayerWriter.class);

  @Getter
  @Setter
  private MongoLayerWriterConfig writerConfig;

  public MongoLayerWriter(SparkSession ss, MongoLayerWriterConfig writerConfig) {
    super(ss);
    this.writerConfig = writerConfig;
  }

  @Override
  public Document transform(Feature f) {
    if (f == null || f.getGeometry() == null) {
      return null;
    }
    return Document.parse(f.toJson());
  }

  /**
   * 注入 MongoDB 写入输出参数，详见官网文档：
   * https://docs.mongodb.com/spark-connector/v2.3/configuration/#output-configuration
   * @param layer
   */
  @Override
  public void write(Layer layer) {
    // Create a custom WriteConfig
    Map<String, String> writeOverrides = new HashMap<String, String>();
    writeOverrides.put("uri", writerConfig.getUri());
    writeOverrides.put("database", writerConfig.getDatabase());
    writeOverrides.put("collection", writerConfig.getCollection());

    WriteConfig writeConfig = WriteConfig.create(writeOverrides);

    JavaRDD<Tuple2<String, Feature>> t = layer.rdd().toJavaRDD();
    JavaRDD<Document> documents = t.map(new Function<Tuple2<String, Feature>, Document>() {
      @Override
      public Document call(Tuple2<String, Feature> t) throws Exception {
        return transform(t._2);
      }
    });

    MongoSpark.save(documents, writeConfig);
  }

}
