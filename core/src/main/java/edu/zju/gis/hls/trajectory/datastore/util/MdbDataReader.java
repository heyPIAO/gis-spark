package edu.zju.gis.hls.trajectory.datastore.util;

import edu.zju.gis.hls.trajectory.datastore.exception.DataReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Feature;
import org.gdal.ogr.FieldDefn;
import org.gdal.ogr.ogr;

import java.io.IOException;

/**
 * @author Hu
 * @date 2020/9/14
 * Gdal Java API：https://gdal.org/java/overview-summary.html
 * Java 版本 Gdal 在 Windows 下的编译：https://trac.osgeo.org/gdal/wiki/GdalOgrInJavaBuildInstructions
 * Gdal Java Samples: https://trac.osgeo.org/gdal/browser/trunk/gdal/swig/java/apps/
 **/
@Getter
@Setter
@Slf4j
public class MdbDataReader extends DataReader {

  private String layerName;
  private org.gdal.ogr.Layer featureLayer;

  public MdbDataReader(String filename, String layerName) {
    this.filename = filename.replace(SourceType.MDB.getPrefix(),"");
    this.layerName = layerName;
  }

  static {
    ogr.RegisterAll();
    gdal.SetConfigOption("MDB_DRIVER_TEMPLATE", "DRIVER=Microsoft Access Driver (*.mdb, *.accdb);DBQ=%s");
    gdal.SetConfigOption("PGEO_DRIVER_TEMPLATE", "DRIVER=Microsoft Access Driver (*.mdb, *.accdb);DBQ=%s");
    gdal.SetConfigOption("GDAL_FILENAME_IS_UTF8", "YES");
//    gdal.SetConfigOption("SHAPE_ENCODING", "CP936");
    gdal.SetConfigOption("SHAPE_ENCODING", "GBK");
  }

  @Override
  public void init() {
    super.init();
    this.initReader();
    this.readHeader();
    this.readCRS();
  }

  private void initReader() {
    DataSource ds = ogr.Open(filename, 0);
    org.gdal.ogr.Layer player = ds.GetLayerByName(layerName);
    if (player == null) {
      throw new DataReaderException(String.format("Invalid layer reader for mdb: %s[%s]", filename, layerName));
    }
    this.featureLayer = player;
  }

  @Override
  public String next() {
    Feature f = this.nextFeature();
    if (f != null) {
      return f.toString();
    }
    return null;
  }

  public Feature nextFeature() {
    return featureLayer.GetNextFeature();
  }

  @Override
  protected String[] readHeader() {
    String[] headers = new String[this.featureLayer.GetLayerDefn().GetFieldCount()];
    for (int i=0; i<headers.length; i++) {
      headers[i] = this.featureLayer.GetLayerDefn().GetFieldDefn(i).toString();
    }
    return headers;
  }

  @Override
  protected void readCRS() {
    this.crs = this.featureLayer.GetSpatialRef().ExportToWkt();
  }

  @Override
  public void close() throws IOException {

  }

  public static Object getField(Feature f, String fieldName) {
    FieldDefn ft = f.GetFieldDefnRef(fieldName);
    if (ft == null) {
      log.error(String.format("Unvalid field name [%s], not exists", fieldName));
      return null;
    }
    return mapToObject(f, ft);
//    String fieldTypeName = ft.GetTypeName();
//    return f.GetFieldAsString(fieldName);
  }

  public static String getId(Feature f) {
    return String.valueOf(f.GetFID());
  }

  // TODO 太丑了
  // TODO gdal.Feature 还支持：byte[], Integer64, List<Integer>, List<Double>, List<String>，Datetime，框架暂不支持
  private static Object mapToObject(Feature f, FieldDefn ft) {
    String fieldTypeName = ft.GetTypeName();
    switch (fieldTypeName) {
      case "Integer": return f.GetFieldAsInteger(ft.GetName());
      case "Double": return f.GetFieldAsDouble(ft.GetName());
      default: return f.GetFieldAsString(ft.GetName());
    }
  }

}
