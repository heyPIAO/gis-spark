package edu.zju.gis.hls.trajectory.datastore.storage.reader;

import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.mongo.MongoLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.platform.PlatformLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.file.FileLayerWriterConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.mongo.MongoLayerWriterConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriterConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Hu
 * @date 2019/9/20
 * 支持的数据源类型
 **/
@Slf4j
public enum SourceType {

  FILE(0, "file://"), MONGODB(1, "mongo://"), SHP(2, "shp://"), ES(3, "es://"),
  HDFS_FILE(4, "hdfs://"), HDFS_SHP(5, "hdfshp://"), PG(6, "pg://"),
  MYSQL(7, "mysql://"), PLATFORM(8, "platform://");

  @Getter
  private int type;

  @Getter
  private Class readerConfigClass;

  @Getter
  private Class writerConfigClass;

  private String prefix;

  SourceType(int type, String prefix) {
    this.type = type;
    this.prefix = prefix;
    this.readerConfigClass = this.getReaderConfigClass(type);
    this.writerConfigClass = this.getLayerWriterConfigClass(type);
  }

  public static SourceType getSourceType(String path) {
    for (SourceType st: SourceType.values()) {
      if (path.startsWith(st.prefix)) return st;
    }
    throw new GISSparkException("Unsupport source type: " + path);
  }

  private Class getReaderConfigClass(int type) {
    switch (type) {
      case 0: return FileLayerReaderConfig.class;
      case 1: return MongoLayerReaderConfig.class;
      case 2: return ShpLayerReaderConfig.class;
      case 3: return ESLayerReaderConfig.class;
      case 4: return FileLayerReaderConfig.class;
      case 6: return PgLayerReaderConfig.class;
      case 8: return PlatformLayerReaderConfig.class;
      default:
        throw new GISSparkException("Unsupport layer reader for type: " + type);
    }
  }

  private Class getLayerWriterConfigClass(int type) {
    switch (type) {
      case 0: return FileLayerWriterConfig.class;
      case 1: return MongoLayerWriterConfig.class;
      case 6: return PgLayerWriterConfig.class;
      default:
        throw new GISSparkException("Unsupport layer writer for type: " + type);
    }
  }

}
