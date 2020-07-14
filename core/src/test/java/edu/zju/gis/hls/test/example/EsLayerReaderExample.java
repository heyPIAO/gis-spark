package edu.zju.gis.hls.test.example;

import edu.zju.gis.dbfg.queryserver.constant.PlatFormStorageType;
import edu.zju.gis.dbfg.queryserver.model.EsConnectInfo;
import edu.zju.gis.dbfg.queryserver.tool.DataFactory;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PointLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.es.ESLayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

@Slf4j
public class EsLayerReaderExample {

    public static void main(String[] args) throws Exception {
        DataFactory pgDf = new DataFactory(PlatFormStorageType.PF, "wmx");
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
