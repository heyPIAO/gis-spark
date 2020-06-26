package edu.zju.gis.hls.test.example;

import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.quadtree.QuadTreeIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.*;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author Hu
 * @date 2020/6/24
 * 将 DLTB、LXDW、XZDW 导出成格网组织
 **/
public class TDLYDataWriter {

  private static final Logger logger = LoggerFactory.getLogger(TDLYDataWriter.class);

  public static void main(String[] args) throws FactoryException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

    // setup spark environment
    SparkSession ss = SparkSession
      .builder()
      .appName("Polygon Layer Demo")
      .master("local[4]")
      .getOrCreate();

    // set up data source
    String dltbFile = "D:\\Work\\STCoding\\测试数据\\净面积计算测试数据\\DLTB";
    // String dltbFile = "D:\\Work\\STCoding\\测试数据\\JIASHAN_2018_DLTB.wkt";
    // String dltbFile = "D:\\Work\\STCoding\\测试数据\\error_polygon.txt";
    String lxdwFile = "D:\\Work\\STCoding\\测试数据\\净面积计算测试数据\\LXDW";
    String xzdwFile = "D:\\Work\\STCoding\\测试数据\\净面积计算测试数据\\XZDW";

    Field shapeField = Term.FIELD_DEFAULT_SHAPE;
    shapeField.setIndex(0);
    Field fid = new Field("FID","FeatureID", 1, FieldType.NORMA_FIELD);

    // 地类图斑数据属性字段设置
    FileLayerReaderConfig dltbReaderConfig = new FileLayerReaderConfig(UUID.randomUUID().toString(), dltbFile, LayerType.MULTI_POLYGON_LAYER);
    dltbReaderConfig.setIdField(fid);
    dltbReaderConfig.setShapeField(shapeField);
    dltbReaderConfig.setAttributes(getDltbFields());
    dltbReaderConfig.setCrs(CRS.parseWKT(Term.WKT_4528));

    FileLayerReaderConfig lxdwReaderConfig = new FileLayerReaderConfig(UUID.randomUUID().toString(), lxdwFile, LayerType.POINT_LAYER);
    lxdwReaderConfig.setIdField(fid);
    lxdwReaderConfig.setShapeField(shapeField);
    lxdwReaderConfig.setAttributes(getLxdwFields());
    lxdwReaderConfig.setCrs(CRS.parseWKT(Term.WKT_4528));

    FileLayerReaderConfig xzdwReaderConfig = new FileLayerReaderConfig(UUID.randomUUID().toString(), xzdwFile, LayerType.POLYLINE_LAYER);
    xzdwReaderConfig.setIdField(fid);
    xzdwReaderConfig.setShapeField(shapeField);
    xzdwReaderConfig.setAttributes(getXzdwFields());
    xzdwReaderConfig.setCrs(CRS.parseWKT(Term.WKT_4528));

    FileLayerReader<MultiPolygonLayer> dltbReader = new FileLayerReader<MultiPolygonLayer>(ss, dltbReaderConfig);
    FileLayerReader<PointLayer> lxdwReader = new FileLayerReader<PointLayer>(ss, lxdwReaderConfig);
    FileLayerReader<PolylineLayer> xzdwReader = new FileLayerReader<PolylineLayer>(ss, xzdwReaderConfig);

    MultiPolygonLayer dltbLayer = dltbReader.read();
    PolylineLayer xzdwLayer = xzdwReader.read();
    PointLayer lxdwLayer = lxdwReader.read();

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.QUADTREE, new QuadTreeIndexConfig(4));
    FileLayerWriterConfig writerConfig = new FileLayerWriterConfig();
    writerConfig.setKeepKey(true);

    // write to file with grid index
    IndexedLayer<MultiPolygonLayer> dltb = si.index(dltbLayer);

    MultiPolygonLayer l = dltb.toLayer();
    l.makeSureCached();
    logger.info("Temp Layer Size = " + l.count());
    l.unpersist();

    writerConfig.setSinkPath("D:\\Work\\STCoding\\净面积计算\\grid\\dltb");
    LayerWriter writer = new FileLayerWriter(ss, writerConfig);
    writer.write(dltb);

    IndexedLayer<PolylineLayer> xzdw = si.index(xzdwLayer);
    writerConfig.setSinkPath("D:\\Work\\STCoding\\净面积计算\\grid\\xzdw");
    writer = new FileLayerWriter(ss, writerConfig);
    writer.write(xzdw);

    IndexedLayer<PointLayer> lxdw = si.index(lxdwLayer);
    writerConfig.setSinkPath("D:\\Work\\STCoding\\净面积计算\\grid\\lxdw");
    writer = new FileLayerWriter(ss, writerConfig);
    writer.write(lxdw);

    ss.close();
  }

  private static Field[] getDltbFields() {
    Field bsm = new Field("BSM","标识码", 2, FieldType.NORMA_FIELD);
    Field dlmc = new Field("DLMC","地类名称", 7, FieldType.NORMA_FIELD);
    Field tbbh = new Field("TBBH", "图斑编号", 5, FieldType.NORMA_FIELD);
    Field zldwdm = new Field("ZLDWDM", "坐落单位代码", 11, FieldType.NORMA_FIELD);
    Field tbmj = new Field("TBMJ", "图斑面积", 19, FieldType.NORMA_FIELD);
    Field tkxs = new Field("TKXS", "田坎系数", 18, FieldType.NORMA_FIELD);
    return new Field[]{bsm, dlmc, tbbh, zldwdm, tbmj, tkxs};
  }

  private static Field[] getXzdwFields() {
    Field bsm = new Field("BSM","标识码", 2, FieldType.NORMA_FIELD);
    Field dlmc = new Field("DLMC","地类名称", 5, FieldType.NORMA_FIELD);
    Field kcbl = new Field("KCBL", "扣除XX0", 21, FieldType.NORMA_FIELD);
    Field kctbbh1 = new Field("KCTBBH1", "扣除图斑编号1", 16, FieldType.NORMA_FIELD);
    Field kctbdwdm1 = new Field("KCTBDWDM1", "扣除图斑地物代码1", 17, FieldType.NORMA_FIELD);
    Field kctbbh2 = new Field("KCTBBH2", "扣除图斑编号2", 18, FieldType.NORMA_FIELD);
    Field kctbdwdm2 = new Field("KCTBDWDM2", "扣除图斑地物代码2", 19, FieldType.NORMA_FIELD);
    Field cd = new Field("CD", "CD", 8, FieldType.NORMA_FIELD);
    Field kd = new Field("KD", "KD", 9, FieldType.NORMA_FIELD);
    Field xzdwmj = new Field("XZDWMJ", "现状地物面积", 10, FieldType.NORMA_FIELD);
    Field shape_length = new Field("SHAPE_LENGTH", "长度", 25, FieldType.NORMA_FIELD);
    return new Field[]{bsm, dlmc, kcbl, kctbbh1, kctbdwdm1, kctbbh2, kctbdwdm2, cd, kd, xzdwmj, shape_length};
  }

  private static Field[] getLxdwFields() {
    Field dlmc = new Field("DLMC","地类名称", 5, FieldType.NORMA_FIELD);
    Field zltbbh = new Field("ZLTBBH", "坐落图斑编号", 9, FieldType.NORMA_FIELD);
    Field zldwdm = new Field("ZLDWDM", "坐落单位代码", 11, FieldType.NORMA_FIELD);
    Field dlbz = new Field("DLBZ", "地类标志", 15, FieldType.NORMA_FIELD);
    Field mj = new Field("MJ", "图斑面积", 10, FieldType.NORMA_FIELD);
    return new Field[]{dlmc, zltbbh, zldwdm, dlbz, mj};
  }

}
