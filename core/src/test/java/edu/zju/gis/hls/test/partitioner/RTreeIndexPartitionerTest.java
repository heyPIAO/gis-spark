package edu.zju.gis.hls.test.partitioner;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.rtree.RTree;
import edu.zju.gis.hls.trajectory.analysis.index.rtree.RTreeIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.index.rtree.RTreePartitioner;
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
import scala.Tuple2;

import java.util.List;

/**
 * @author Hu
 * @date 2020/8/25
 **/
@Slf4j
public class RTreeIndexPartitionerTest {

  public static void main(String[] args) throws Exception {

    SparkSession ss = SparkSession.builder()
      .master("local[1]")
      .appName("RTreePartitionerTest")
      .getOrCreate();

    Field shapeField = Term.FIELD_DEFAULT_SHAPE;
    shapeField.setIndex(0);
    Field fid = new Field("FID","FeatureID", 1, FieldType.ID_FIELD);

    String dataFile = "file:///E:\\2020projects\\gis-spark\\core\\src\\test\\resources\\Province.csv";

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

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.RTREE, new RTreeIndexConfig((int)(provinceLayer.count() * 0.1)));
    KeyIndexedLayer<MultiPolygonLayer> indexedProvinceLayer = si.index(provinceLayer);

    indexedProvinceLayer.makeSureCached();
    RTreePartitioner partitioner = (RTreePartitioner) indexedProvinceLayer.getPartitioner();
    RTree rTree = partitioner.getRTree();
    printInfo(String.valueOf(rTree.isFinish()));

    // 查看均匀格网索引下的 province layer 图层
    printInfo(" ===== INDEXED LAYER START ===== ");
    printInfo("partition info: \n" );
    indexedProvinceLayer.getLayer().partitions().forEach(x->printInfo(x.toString()));

    printInfo("rdd partitioner: \n");
    printInfo(indexedProvinceLayer.getLayer().partitioner().get().toString());

    printInfo("key range feature: \n");
    indexedProvinceLayer.getPartitioner().getKeyRanges().forEach((x1, x2)-> {
      printInfo(String.format("%s: %s", x1, x2.toString()));
    });

    indexedProvinceLayer.makeSureCached();

    List<Tuple2<String, MultiPolygon>> r = indexedProvinceLayer.getLayer().take(10);
    r.forEach(x->printInfo(x._1 + "\t" + x._2.toString()));

    printInfo(" ===== INDEXED LAYER FINISH ===== ");

    ss.stop();
    ss.close();
  }

  private static Field[] getProvinceFields() {
    Field xzqdm = new Field("XZQDM","行政区代码", 2, FieldType.NORMAL_FIELD);
    Field xzqmc = new Field("XZQMC", "行政区名称", 3, FieldType.NORMAL_FIELD);
    return new Field[]{xzqdm, xzqmc};
  }

  public static void printInfo(String s) {
    System.out.println(s);
  }

}
