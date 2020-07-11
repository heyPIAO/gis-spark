package edu.zju.gis.hls.gisspark.model.basicop;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.args.ClipArgs;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.operate.ClipOperator;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

/**
 * @author Hu
 * @date 2020/7/10
 **/
@Slf4j
public class Clip extends BaseModel<ClipArgs> {

  public Clip(String[] args) {
    super(args);
  }

  @Override
  public void run() throws Exception {
    LayerReaderConfig config1 = LayerFactory.getReaderConfig(this.arg.getInput1());
    LayerReader layerReader1 = LayerFactory.getReader(this.ss, config1, config1.getLayerType().getLayerClass());
    Layer<String, Feature> layer1 = (Layer<String, Feature>)layerReader1.read();

    List<Feature> fs = Arrays.asList(layer1.collectAsMap().values().toArray(new Feature[]{}));
    if (fs.size() > 1) {
      throw new GISSparkException("Unsupport multi feature as an extent clip layer yet");
    }
    Feature f = fs.get(0);

    LayerReaderConfig config2 = LayerFactory.getReaderConfig(this.arg.getInput2());
    LayerReader layerReader2 = LayerFactory.getReader(this.ss, config2, config2.getLayerType().getLayerClass());
    Layer layer2 = layerReader2.read();

    ClipOperator clip = new ClipOperator(this.ss, arg.isAttrReserved());
    Layer l = clip.run(f, layer2);

    l.print();

    LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getOutput());
    LayerWriter writer = LayerFactory.getWriter(ss, writerConfig);
    writer.write(l);
  }

  @Override
  protected void finish() {
    super.finish();
    log.info("Clip Finish with args: " + this.arg.toString());
  }

  public static void main(String[] args) throws Exception {
    Clip clip = new Clip(args);
    clip.exec();
  }

}
