package edu.zju.gis.hls.test.example;

import edu.zju.gis.dbfg.queryserver.constant.PlatFormStorageType;
import edu.zju.gis.dbfg.queryserver.model.EsConnectInfo;
import edu.zju.gis.dbfg.queryserver.tool.DataFactory;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.es.ESLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.es.ESLayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.NORMA_FIELD;

@Slf4j
public class EsLayerReaderExample {

    public static void main(String[] args) throws Exception {
        String esInfo = "{\n" +
                "    \"hosts\":[192.168.1.112],\n" +
                "    \"masterNode\":\"192.168.1.112\",\n" +
                "    \"port\":9200,\n" +
                "    \"index\":\"wmx3\",\n" +
                "}\n";
//        DataFactory pgDf = new DataFactory(PlatFormStorageType.ES, esInfo);
        DataFactory pgDf = new DataFactory(PlatFormStorageType.PF, "5365ef90-0df8-4be4-8289-f619c52bd970");
        EsConnectInfo esConnectInfo = (EsConnectInfo) pgDf.getSchema();

        String source = esConnectInfo.toString();
        String masterNode = esConnectInfo.getMasterNode();
        String[] nodes = esConnectInfo.getHosts();
        String port = String.valueOf(esConnectInfo.getPort());
        String indexName = esConnectInfo.getIndex();
        String typeName = esConnectInfo.getType();

        String pkName = esConnectInfo.getPkField().getName();
        String spatialFieldName = esConnectInfo.getSpatialFields().get(0).getName();
        FeatureType featureType = FeatureType.valueOf(esConnectInfo.getSpatialFields().get(0).getFormat());
        LayerType layerType = LayerType.findLayerType(featureType);

        // setup spark environment
        SparkSession ss = SparkSession
                .builder()
                .appName("Es Layer Reader Demo")
                .master("local[1]")
                .getOrCreate();

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

        List<edu.zju.gis.dbfg.queryserver.model.Field> sourceFields = esConnectInfo.getNormalFields();
        Field[] targetFields = new Field[1];
        targetFields[0] = new Field(sourceFields.get(1).getName(),NORMA_FIELD);
        config.setAttributes(targetFields);

        // read layer
        ESLayerReader<MultiPolygonLayer> layerReader = new ESLayerReader<MultiPolygonLayer>(ss, config);
        MultiPolygonLayer layer = layerReader.read();

        // check if success
        layer.makeSureCached();
        log.info("Layer count: " + layer.count());

//        List<Tuple2<String, MultiPolygon>> features = layer.collect();
//        features.forEach(x -> log.info(x._2.toJson()));

        //Write
        ESLayerWriterConfig w_config = new ESLayerWriterConfig(masterNode, port, "w_test", "_doc");
        ESLayerWriter writer = new ESLayerWriter(ss, w_config);
        writer.write(layer);

        layer.release();
    }
}
