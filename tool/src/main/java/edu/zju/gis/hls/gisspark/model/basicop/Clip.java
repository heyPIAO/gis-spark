package edu.zju.gis.hls.gisspark.model.basicop;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.args.ClipArgs;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.rectGrid.RectGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.operate.ClipOperator;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

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
    LayerReader layerReader1 = LayerFactory.getReader(this.ss, config1);
    Layer<String, Feature> layer1 = (Layer<String, Feature>)layerReader1.read();

    LayerReaderConfig config2 = LayerFactory.getReaderConfig(this.arg.getInput2());
    LayerReader layerReader2 = LayerFactory.getReader(this.ss, config2);
    Layer layer2 = layerReader2.read();

    if (!config1.getCrs().equals(config2.getCrs())) {
      layer1 = layer1.transform(config2.getCrs());
    }
    List<Feature> fs = layer1.collect().stream().map(x->x._2).collect(Collectors.toList());

    ClipOperator clip = new ClipOperator(this.ss, arg.getAttrReserved());

    Layer result = null;

    if (this.arg.getIndexBaseLayer()) {
      DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.RECT_GRID, new RectGridIndexConfig(4));
      IndexedLayer il = si.index(layer2);
      result = clip.run(fs, il);
    } else {
      result = clip.run(fs, layer2);
    }

    LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getOutput());
    LayerWriter writer = LayerFactory.getWriter(ss, writerConfig);
    writer.write(result);
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
