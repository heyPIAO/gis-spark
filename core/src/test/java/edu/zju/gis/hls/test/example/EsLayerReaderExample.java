package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.MultiPolygon;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

@Slf4j
public class EsLayerReaderExample {

    public static void main(String[] args) throws Exception {

        // setup spark environment
        SparkSession ss = SparkSession
                .builder()
                .appName("Es Layer Reader Demo")
                .master("local[1]")
                .config("es.nodes", "192.168.1.112")
                .config("es.port", "9200")
                .config("es.index.read.missing.as.empty", "true")
                .config("es.nodes.wan.only", "true")
                .getOrCreate();

        String source = "http://192.168.1.112:9200/";
        String masterNode = "192.168.1.112";
        String[] nodes = new String[]{"192.168.1.112"};
        String port = "9200";
        String indexName = "wmx3";
        String typeName = "2";

        String pkName = "OBJECTID";
        String spatialFieldName = "GEOM";
        LayerType layerType = LayerType.MULTI_POLYGON_LAYER;

        Field fid = new Field(pkName, FieldType.ID_FIELD);
//        fid.setType(String.class);
        Field shapeField = new Field(spatialFieldName, FieldType.SHAPE_FIELD);

        // set up layer reader config
        ESLayerReaderConfig config = new ESLayerReaderConfig("MultiPolygon_Layer_Test", source, layerType);
        config.setMasterNode(masterNode);
        config.setNodes(nodes);
        config.setPort(port);
        config.setIndexName(indexName);
        config.setTypeName(typeName);
        config.setIdField(fid);
        config.setShapeField(shapeField);

        // read layer
        ESLayerReader<MultiPolygonLayer> layerReader = new ESLayerReader<MultiPolygonLayer>(ss, config);
        MultiPolygonLayer layer = layerReader.read();

        // check if success
        layer.makeSureCached();
        log.info("Layer count: " + layer.count());

        List<Tuple2<String, MultiPolygon>> features = layer.collect();
        features.forEach(x -> log.info(x._2.toJson()));

        layer.release();
    }
}
