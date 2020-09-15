package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;

/**
 * @author Hu
 * @date 2020/9/15
 * 二调数据关联生成merge表
 **/
public class LandFlowPreProcess extends BaseModel<LandFlowPreProcessArgs> {

  public LandFlowPreProcess(SparkSessionType type, String[] args) {
    super(type, args);
  }

  @Override
  protected void run() throws Exception {

    LayerReaderConfig lxdwConfig = LayerFactory.getReaderConfig(this.arg.getLxdwReaderConfig());
    LayerReaderConfig xzdwConfig = LayerFactory.getReaderConfig(this.arg.getXzdwReaderConfig());
    LayerReaderConfig dltbConfig = LayerFactory.getReaderConfig(this.arg.getDltbReaderConfig());

    LayerReader lxdwReader = LayerFactory.getReader(this.ss, lxdwConfig);
    LayerReader xzdwReader = LayerFactory.getReader(this.ss, xzdwConfig);
    LayerReader dltbReader = LayerFactory.getReader(this.ss, dltbConfig);

    Layer lxdwLayer = lxdwReader.read();
    Layer xzdwLayer = xzdwReader.read();
    Layer dltbLayer = dltbReader.read();

    // TODO 根据字段关联

    LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getWriterConfig());
    LayerWriter writer = LayerFactory.getWriter(this.ss, writerConfig);
    // writer.write();
  }


  public static void main(String[] args) throws Exception {
    LandFlowPreProcess preProcess = new LandFlowPreProcess(SparkSessionType.LOCAL, args);
    preProcess.exec();
  }
}
