package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.VectorTileGenerator;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhou
 * @date 2020/8/1
 * VectorTileGenerator Test
 **/
@Slf4j
public class VTGenerateExample {

    public static void main(String[] args) throws Exception{

        // Setup SparkSession
        SparkSession ss = SparkSession
                .builder()
                .appName("Vector Tile Generater Demo")
                .master("local[16]")
                .getOrCreate();

        // Read Kecheng data from shp
        MultiPolygonLayer layer = shpLayerRead(ss);
        layer.cache();
        log.info(String.format("DLTB_2016Kecheng has %d partitions.", layer.count()));

        // Get envelope
        double minX, maxX, minY, maxY;
        JavaRDD<List<Double>> eachExtent = layer.map(x ->{
            List<Double> tempExtent = new ArrayList<>();
            Envelope tempEnvelope = x._2.getGeometry().getEnvelopeInternal();
            tempExtent.add(tempEnvelope.getMinX());
            tempExtent.add(tempEnvelope.getMaxX());
            tempExtent.add(tempEnvelope.getMinY());
            tempExtent.add(tempEnvelope.getMaxY());
            return tempExtent;
        });
        List<Double> extent = eachExtent.reduce((x, y) -> {
            List<Double> tempExtent = new ArrayList<>();
            tempExtent.add(Math.min(x.get(0), y.get(0)));
            tempExtent.add(Math.max(x.get(1), y.get(1)));
            tempExtent.add(Math.min(x.get(2), y.get(2)));
            tempExtent.add(Math.max(x.get(3), y.get(3)));
            return tempExtent;
        });
        log.info("Evelope: " + extent.toString());

        // Setup PyramidConfig
        PyramidConfig pConfig = new PyramidConfig.PyramidConfigBuilder()
                .setBaseMapEnv(extent.get(0), extent.get(1), extent.get(2), extent.get(3))
                .setCrs(layer.getMetadata().getCrs())
                .setzLevelRange(4, 6)
                .build();

        // Setup KeyIndexedLayer
        DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridIndexConfig(pConfig.getZMax()));
        KeyIndexedLayer<MultiPolygonLayer> keyLayer = si.index(layer);

        // Generate vectorTile
        String outDir = "E:\\Postgraduate\\Data\\vectorTile";
        VectorTileGenerator vtGenerator = new VectorTileGenerator(pConfig);
        vtGenerator.generate(keyLayer, outDir);

        ss.stop();
        ss.close();
    }

    private static MultiPolygonLayer shpLayerRead(SparkSession ss) throws Exception{
        String layerName = "DLTB_2016Kecheng";
        String source = "E:\\Postgraduate\\Data\\Kecheng\\DLTB_2016Kecheng_part.shp";
        ShpLayerReaderConfig srConfig = new ShpLayerReaderConfig(layerName, source, LayerType.MULTI_POLYGON_LAYER);

        Field dlbmField = new Field("DLBM", FieldType.NORMAL_FIELD);
        Field[] attributes = { dlbmField };
        srConfig.setAttributes(attributes);

        ShpLayerReader<MultiPolygonLayer> sReader = new ShpLayerReader(ss, srConfig);
        MultiPolygonLayer layer = sReader.read();

        return layer;
    }

}