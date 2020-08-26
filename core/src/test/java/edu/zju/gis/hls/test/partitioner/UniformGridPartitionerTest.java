package edu.zju.gis.hls.test.partitioner;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.DistributeSpatialPartitioner;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.opengis.referencing.FactoryException;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * @author Hu
 * @date 2020/8/25
 **/
@Slf4j
public class UniformGridPartitionerTest {

  public static void main(String[] args) throws FactoryException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {


    SparkSession ss = SparkSession.builder()
      .master("local[4]")
      .appName("UniformGridParitionerTest")
      .getOrCreate();

    Field shapeField = Term.FIELD_DEFAULT_SHAPE;
    shapeField.setIndex(0);
    Field fid = new Field("FID","FeatureID", 1, FieldType.ID_FIELD);

    String dataFile = "file:///D:\\Work\\DaLunWen\\code\\trajectory-spark\\core\\src\\test\\resources\\Province.csv";

    LayerReaderConfig config = new FileLayerReaderConfig();
    config.setLayerId("Province_uniform");
    config.setLayerName("Province_uniform");
    config.setCrs(Term.DEFAULT_CRS);
    config.setSourcePath(dataFile);
    config.setLayerType(LayerType.MULTI_POLYGON_LAYER);
    config.setIdField(fid);
    config.setShapeField(shapeField);
    config.setAttributes(getProvinceFields());

    LayerReader<MultiPolygonLayer> provinceLayerReader = LayerFactory.getReader(ss, config);
    MultiPolygonLayer provinceLayer = provinceLayerReader.read();

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridIndexConfig(8));
    KeyIndexedLayer<MultiPolygonLayer> indexedProvinceLayer = si.index(provinceLayer);

    // 查看均匀格网索引下的 province layer 图层
    log.info(" ===== INDEXED LAYER START ===== ");
    log.info("partition info: \n" );
    indexedProvinceLayer.getLayer().partitions().forEach(x->log.info(x.toString()));

    log.info("rdd partitioner: \n");
    log.info(indexedProvinceLayer.getLayer().partitioner().get().toString());

    log.info("key range feature: \n");
    indexedProvinceLayer.getPartitioner().getKeyRanges().forEach((x1, x2)-> {
      log.info(String.format("%s: %s", x1, x2.toString()));
    });

    DistributeSpatialPartitioner partitioner = indexedProvinceLayer.getPartitioner();
    log.info(partitioner.toString());

    List<Tuple2<String, MultiPolygon>> features = indexedProvinceLayer.getLayer().collect();
    features.forEach(x->{
      log.info(String.format("%s: %s", x._1, x._2));
    });

    log.info(" ===== INDEXED LAYER FINISH ===== ");

    ss.stop();
    ss.close();
  }

  private static Field[] getProvinceFields() {
    Field xzqdm = new Field("XZQDM","行政区代码", 2, FieldType.NORMA_FIELD);
    Field xzqmc = new Field("XZQMC", "行政区名称", 3, FieldType.NORMA_FIELD);
    return new Field[]{xzqdm, xzqmc};
  }

}
