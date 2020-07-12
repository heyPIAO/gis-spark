package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.quadtree.QuadTreeIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.IndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.PolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.file.FileLayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.file.FileLayerWriterConfig;
import org.apache.spark.sql.SparkSession;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

/**
 * @author Hu
 * @date 2020/6/22
 * Polygon 图层读取 + 格网号索引 + 写出
 **/
public class PolygonLayerDemo {

  private static final Logger logger = LoggerFactory.getLogger(PolygonLayerDemo.class);

  public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, FactoryException {

    // setup spark environment
    SparkSession ss = SparkSession
      .builder()
      .appName("Polygon Layer Demo")
      .master("local[4]")
      .getOrCreate();

    // set up data source
    String file = "D:\\Work\\STCoding\\测试数据\\JIASHAN_2018_DLTB.wkt";

    // set up layer reader
    Field fid = new Field("FID","FeatureID", 1, FieldType.NORMA_FIELD);
    Field area = new Field("Area", "面积", 19, FieldType.NORMA_FIELD);
    Field shapeField = Term.FIELD_DEFAULT_SHAPE;
    shapeField.setIndex(0);

    Field[] fields = new Field[]{area};
    FileLayerReaderConfig readerConfig = new FileLayerReaderConfig(UUID.randomUUID().toString(), file, LayerType.POLYGON_LAYER);
    readerConfig.setIdField(fid);
    readerConfig.setShapeField(shapeField);
    readerConfig.setAttributes(fields);
    readerConfig.setCrs(CRS.parseWKT(Term.WKT_4528));

    FileLayerReader<PolygonLayer> reader = new FileLayerReader<PolygonLayer>(ss, readerConfig);

    PolygonLayer layer = reader.read();

    layer.cache();

    // count feature number
    long count = layer.count();
    logger.info("Feature Num = " + count);

    // print data
    // layer.collect().forEach(x->System.out.println(x._2.toString()));

    // transform to key-indexed layer with quadtree
    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.QUADTREE, new QuadTreeIndexConfig(5));
    IndexedLayer<PolygonLayer> til = si.index(layer);

    // write to hdfs with grid index
    FileLayerWriterConfig writerConfig = new FileLayerWriterConfig("/test", true);

    LayerWriter writer = new FileLayerWriter(ss, writerConfig);
    writer.write(til);

    layer.unpersist();

    ss.close();
  }

}
