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
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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
    private final static String TIMEPATTERN = "yyyy/MM/dd HH:mm:ss";

    public MdbDataReader(String filename, String layerName) {
        this.filename = filename.replace(SourceType.MDB.getPrefix(), "");
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
        log.info("Layer Count: " + ds.GetLayerCount());
        for (int i = 0; i < ds.GetLayerCount(); i++) {
            log.info(ds.GetLayerByIndex(i).GetName());
        }
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
        for (int i = 0; i < headers.length; i++) {
            headers[i] = this.featureLayer.GetLayerDefn().GetFieldDefn(i).toString();
        }
        return headers;
    }

    @Override
    protected String readCRS() {
        return this.featureLayer.GetSpatialRef().ExportToWkt();
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
    }

    public static String getId(Feature f) {
        return String.valueOf(f.GetFID());
    }

    // TODO 太丑了
    // TODO gdal.Feature 还支持：byte[], Integer64, List<Integer>, List<Double>, List<String>，Datetime，框架暂不支持
    // TODO moral: GetFieldAsDateTime方法太反人类了，写死了一个解析方案
    private static Object mapToObject(Feature f, FieldDefn ft) {
        String fieldTypeName = ft.GetTypeName();
        switch (fieldTypeName) {
            case "Integer":
                return f.GetFieldAsInteger(ft.GetName());
            case "Double":
            case "Real":
                return f.GetFieldAsDouble(ft.GetName());
            case "DateTime":
                String timeStr = f.GetFieldAsString(ft.GetName());
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(TIMEPATTERN);
                java.sql.Date dateTime = null;
                try {
                    dateTime = new java.sql.Date(simpleDateFormat.parse(timeStr).getTime());
                } catch (ParseException e) {
                    dateTime = new java.sql.Date(779890394000l);
                }
                return dateTime;
            case "String":
                String str = f.GetFieldAsString(ft.GetName());
                byte[] bs = new byte[0];
                try {
                    bs = str.getBytes("GB2312");
                    return new String(bs, StandardCharsets.UTF_8);
                } catch (UnsupportedEncodingException e) {
                    log.error("字符GBK->GBK失败：" + e.getMessage());
                    return f.GetFieldAsString(ft.GetName());
                }
                //用新的字符编码生成字符串
            default:
                return f.GetFieldAsString(ft.GetName());
        }
    }
}
