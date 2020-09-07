package edu.zju.gis.hls.trajectory.datastore.storage;

import com.google.gson.Gson;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerWriterException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.file.FileLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.file.FileLayerWriterConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriterConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

/**
 * @author Hu
 * @date 2020/7/10
 **/
public class LayerFactory {

  /**
   * TODO 如何动态传递读取的图层类型？？现在的方法有点蠢
   * @param ss
   * @param config
   * @return
   */
  public static <L extends Layer> LayerReader<L> getReader(SparkSession ss, LayerReaderConfig config) {

    SourceType sourceType = SourceType.getSourceType(config.getSourcePath());
    LayerType layerType = config.getLayerType();

    if (sourceType.equals(SourceType.FILE) || sourceType.equals(SourceType.HDFS_FILE)) {
      return new FileLayerReader<L>(ss, (FileLayerReaderConfig) config);
    } else if (sourceType.equals(SourceType.PG)) {
      return new PgLayerReader<L>(ss, (PgLayerReaderConfig) config);
    } else if (sourceType.equals(SourceType.ES)) {
      return new ESLayerReader<L>(ss, (ESLayerReaderConfig) config);
    } else if (sourceType.equals(SourceType.SHP)) {
      return new ShpLayerReader<L>(ss, (ShpLayerReaderConfig) config);
    } else {
      throw new LayerReaderException("Unsupport layer reader type: " + layerType.name());
    }
  }

  public static LayerWriter getWriter(SparkSession ss, LayerWriterConfig config) {

    SourceType sourceType = SourceType.getSourceType(config.getSinkPath());

    if (sourceType.equals(SourceType.FILE) || sourceType.equals(SourceType.HDFS_FILE)) {
      return new FileLayerWriter (ss, (FileLayerWriterConfig) config);
    } else if (sourceType.equals(SourceType.PG)) {
      return new PgLayerWriter (ss, (PgLayerWriterConfig) config);
    } else {
      throw new LayerWriterException("Unsupport layer writer type: " + config.getClass().getName());
    }
  }

  /**
   * 从 json 字符串中反序列化出对应的 config 对象
   * @param json
   * @return
   */
  public static LayerReaderConfig getReaderConfig(String json) {
    JSONObject jo = new JSONObject(json);
    SourceType sourceType = SourceType.getSourceType(jo.getString("sourcePath"));
    LayerReaderConfig r = (LayerReaderConfig) Term.GSON_CONTEXT.fromJson(jo.toString(), sourceType.getReaderConfigClass());
    return r;
  }

  /**
   * 从 json 字符串中反序列化出对应的 config 对象
   * @param json
   * @return
   */
  public static LayerWriterConfig getWriterConfig(String json) {
    JSONObject jo = new JSONObject(json);
    SourceType sourceType = SourceType.getSourceType(jo.getString("sinkPath"));
    Gson gson = new Gson();
    return (LayerWriterConfig) gson.fromJson(json, sourceType.getLayerWriterConfigClass());
  }

}
