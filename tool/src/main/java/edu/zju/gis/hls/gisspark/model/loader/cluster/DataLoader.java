package edu.zju.gis.hls.gisspark.model.loader.cluster;

import com.google.gson.Gson;
import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.args.DataLoaderArgs;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author Hu
 * @date 2020/7/14
 * 数据入库到指定后端
 **/
@Slf4j
public class DataLoader<A extends DataLoaderArgs> extends BaseModel<A> {

    public DataLoader(String[] args) {
        super(args);
    }

    public DataLoader(SparkSessionType type, String[] args) {
        super(type, args);
    }

    protected LayerMetadata metadata = null;

    @Override
    protected void run() throws Exception {
        try {
            LayerReaderConfig readerConfig = LayerFactory.getReaderConfig(this.arg.getInput());
            LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getOutput());

            LayerReader layerReader = LayerFactory.getReader(this.ss, readerConfig);
            Layer<String, Feature> layer = (Layer<String, Feature>) layerReader.read();

//    CoordinateReferenceSystem targetCrs = CRS.decode(this.arg.getTargetCrs());
//    if (!layer.getMetadata().getCrs().equals(targetCrs))
//      layer = layer.transform(targetCrs);

            // 计算图层四至
            layer.makeSureCached();
            layer.analyze();
            metadata = layer.getMetadata();
            log.info(metadata.getAttributes().toString());
            storeMetadata(metadata);

            LayerWriter writer = LayerFactory.getWriter(ss, writerConfig);
            writer.write(layer);
        } catch (Exception e) {
            log.error("Load error:" + e.getMessage());
            catchError(e);
        }
    }

    protected void storeMetadata(LayerMetadata metadata) {
        log.info("Store Layer Metadata for Layer " + metadata.getLayerName() + ": " + metadata.toJson());
        log.info("Layer Count:" + metadata.getLayerCount());
    }

    protected void catchError(Exception e) {
        log.error("Load Error: " + e.getMessage());
    }

    @Override
    protected void finish() {
        log.info("DataLoader Job Finish");
        super.finish();
    }

    public static void main(String[] args) throws Exception {
        DataLoader pgLoader = new DataLoader(SparkSessionType.LOCAL, args);
        pgLoader.exec();
    }

}
