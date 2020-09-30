package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.VectorTileGenerator;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/**
 * @author Zhou
 * @date 2020/8/1
 * VectorTileGenerator Test
 **/
@Slf4j
public class VTGenerateExample {

    public static void main(String[] args) throws Exception{

        // Set base param
        int zMin = 9;
        int zMax = 12;
        String fileType = "mvt";
        String layerName = "LCRA";
        String source = "E:\\Data\\shp\\LCRA.shp";
        String outDir = "E:\\Data\\vectorTileMVT";

        // Setup SparkSession
        SparkSession ss = SparkSession
                .builder()
                .appName("Vector Tile Generater Demo")
                .master("local[*]")
                .getOrCreate();

        // Read Kecheng data from shp
        MultiPolygonLayer layer = shpLayerRead(ss, layerName, source);
        layer.cache();
        log.info(String.format("%s has %d partitions.", layerName, layer.count()));

        // Setup KeyIndexedLayer
        DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridIndexConfig(zMax));
        KeyIndexedLayer<MultiPolygonLayer> keyLayer = si.index(layer);

        // Generate vectorTile
        VectorTileGenerator vtGenerator = new VectorTileGenerator(zMin, zMax , fileType);
        vtGenerator.generate(keyLayer, outDir);

        ss.stop();
        ss.close();
    }

    private static MultiPolygonLayer shpLayerRead(SparkSession ss, String layerName, String source) throws Exception{
        ShpLayerReaderConfig srConfig = new ShpLayerReaderConfig(layerName, source, LayerType.MULTI_POLYGON_LAYER);
        ShpLayerReader<MultiPolygonLayer> sReader = new ShpLayerReader(ss, srConfig);
        MultiPolygonLayer layer = sReader.read();
        return layer;
    }

}