package com.katus;

import edu.zju.gis.hls.trajectory.PyramidTileGenerator;
import edu.zju.gis.hls.trajectory.analysis.index.rectGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.index.rectGrid.RectGridIndex;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.*;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import org.apache.spark.sql.SparkSession;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-09-16
 */
public class TileTest {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException, IOException, FactoryException {
        // setup spark environment
        SparkSession ss = SparkSession
                .builder()
                .appName("Tile Demo")
                .master("local[*]")
                .getOrCreate();
        // read the data
        FileLayerReaderConfig readerConfig = new FileLayerReaderConfig("LCRA", "file:///D:\\Data\\WKT\\LCRA-t.csv", LayerType.MULTI_POLYGON_LAYER);
        // 配置属性值
        Field[] fields = new Field[2];
        fields[0] = new Field("CC", 1, FieldType.NORMAL_FIELD);
        fields[1] = new Field("AREACODE", 2, FieldType.NORMAL_FIELD);
        readerConfig.setAttributes(fields);
        // 配置几何字段
        readerConfig.setShapeField(new Field("WKT", "shape_field", Geometry.class.getName(), 0, 3, FieldType.SHAPE_FIELD));
        // 配置ID字段
        readerConfig.setIdField(new Field("OBJECTID", "id_field", String.class.getName(), 0, 0, FieldType.ID_FIELD));
        // 设置地理参考
        readerConfig.setCrs(CRS.decode("epsg:3857"));
        // 读取文件
        FileLayerReader<MultiPolygonLayer> fileLayerReader = new FileLayerReader<MultiPolygonLayer>(ss, readerConfig);
        MultiPolygonLayer layer = fileLayerReader.read();
//        layer.analyze();
//        LayerMetadata meta = layer.getMetadata();
        // index the layer
        RectGridIndex rectGridIndex = new RectGridIndex();
        KeyIndexedLayer<MultiPolygonLayer> qLayer = rectGridIndex.index(layer);
        // build the pyramid
        PyramidConfig.PyramidConfigBuilder builder = new PyramidConfig.PyramidConfigBuilder();
        PyramidConfig pyramidConfig = builder
                .setBaseMapEnv(-20026376.39, -20026376.39, -20026376.39, -20026376.39)
                .setZLevelRange(10, 14)
                .setCrs(layer.getMetadata().getCrs())
                .setColorFieldName("CC")
                .setColorSymbol("viz/src/test/resources/color.properties")
                .setBaseDir("D:\\Data\\tiles2")
                .build(false);
        PyramidTileGenerator pyramidTileGenerator = new PyramidTileGenerator(ss, pyramidConfig);
        pyramidTileGenerator.operate(qLayer);
    }
}
