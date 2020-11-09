package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.VectorTileGenerator;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.MultiPolygon;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.shp.ShpLayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.geotools.referencing.CRS;
import scala.Tuple2;

import java.io.File;
import java.io.FileReader;

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
        String source = "E:\\Data\\shp\\LCRA_4326.shp";
        String outDir = "E:\\Data\\vectorTileMVT";

        // Setup SparkSession
        SparkSession ss = SparkSession
                .builder()
                .appName("Vector Tile Generater Demo")
                .master("local[*]")
                .getOrCreate();

        String rawCrs = "WKT_3857";
        File file = new File(source.replace(".shp", ".prj"));
        FileReader fr = new FileReader(file);
        char[] buffer = new char[6];
        if(fr.read(buffer) == 6){
            if(String.valueOf(buffer).equals("GEOGCS")) rawCrs = "WKT_4326";
        }

        // Read Kecheng data from shp
        MultiPolygonLayer layer = shpLayerRead(ss, layerName, source);

        if(rawCrs.equals("WKT_4326")){
            layer = (MultiPolygonLayer) layer.mapToLayer(new Function<Tuple2<String, MultiPolygon>, Tuple2<String, MultiPolygon>>() {
                @Override
                public Tuple2<String, MultiPolygon> call(Tuple2<String, MultiPolygon> t) throws Exception {
                    t._2.transform(CRS.parseWKT(Term.WKT_4326), CRS.parseWKT(Term.WKT_3857));
                    return t;
                }
            });
        }

        layer.getMetadata().setCrs(CRS.parseWKT(Term.WKT_3857));
        layer.cache();
        log.info(String.format("%s has %d partitions.", layerName, layer.count()));

        // Setup KeyIndexedLayer
        DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridIndexConfig(zMax));
        KeyIndexedLayer<MultiPolygonLayer> keyLayer = si.index(layer);

        // Show sample
        keyLayer.getLayer().rdd().toJavaRDD().map(x -> x._1).take(10).forEach(System.out::println);

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