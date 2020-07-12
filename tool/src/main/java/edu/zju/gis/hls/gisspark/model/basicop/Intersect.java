package edu.zju.gis.hls.gisspark.model.basicop;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.args.IntersectArgs;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.operate.IntersectOperator;
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
public class Intersect extends BaseModel<IntersectArgs> {

  public Intersect(String[] args) {
    super(args);
  }

  @Override
  public void run() throws Exception {
    LayerReaderConfig config1 = LayerFactory.getReaderConfig(this.arg.getInput1());
    LayerReader layerReader1 = LayerFactory.getReader(this.ss, config1, config1.getLayerType().getLayerClass());
    Layer<String, Feature> layer1 = (Layer<String, Feature>)layerReader1.read();

    LayerReaderConfig config2 = LayerFactory.getReaderConfig(this.arg.getInput2());
    LayerReader layerReader2 = LayerFactory.getReader(this.ss, config2, config2.getLayerType().getLayerClass());
    Layer layer2 = layerReader2.read();

    if (!config1.getCrs().equals(config2.getCrs())) {
      layer1 = layer1.transform(config2.getCrs());
    }

    List<Feature> fs = Arrays.asList(layer1.collectAsMap().values().toArray(new Feature[]{}));
    if (fs.size() > 1) {
      throw new GISSparkException("Unsupport multi feature as an extent intersect layer yet");
    }
    Feature f = fs.get(0);

    IntersectOperator intersectOp = new IntersectOperator(this.ss, arg.isAttrReserved());
    Layer l = intersectOp.run(f, layer2);

    l.print();

    LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getOutput());
    LayerWriter writer = LayerFactory.getWriter(ss, writerConfig);
    writer.write(l);
  }

  @Override
  protected void finish() {
    super.finish();
    log.info("Intersect Job Finish with args: " + this.arg.toString());
  }

  public static void main(String[] args) throws Exception {
    Intersect intersect = new Intersect(args);
    intersect.exec();
  }

}
