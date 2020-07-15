package edu.zju.gis.hls.gisspark.model.loader.cluster;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.args.DataLoaderArgs;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Hu
 * @date 2020/7/14
 * 数据入库到 PostgreSQL
 **/
@Slf4j
public class DataLoader extends BaseModel<DataLoaderArgs> {

  public DataLoader(String[] args) {
    super(args);
  }

  @Override
  protected void run() throws Exception {

    LayerReaderConfig readerConfig = LayerFactory.getReaderConfig(this.arg.getInput());
    LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getOutput());

    LayerReader layerReader = LayerFactory.getReader(this.ss, readerConfig, readerConfig.getLayerType().getLayerClass());
    Layer<String, Feature> layer = (Layer<String, Feature>)layerReader.read();

    LayerWriter writer = LayerFactory.getWriter(ss, writerConfig);
    writer.write(layer);

  }

  @Override
  protected void finish() {
    log.info("DataLoader Job Finish");
    super.finish();
  }

  public static void main(String[] args) throws Exception {
    DataLoader pgLoader = new DataLoader(args);
    pgLoader.exec();
  }

}
