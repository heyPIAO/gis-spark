package edu.zju.gis.hls.trajectory.datastore.storage.reader.pg;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.util.Converter;
import edu.zju.gis.hls.trajectory.datastore.exception.LayerReaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * @author Hu
 * @date 2020/7/1
 * 读取 Postgis 数据源中的数据图层
 * Hint：
 * （1）虽然Pg支持每张表可以有多种空间类型字段，但是框架目前每个图层仅支持一种空间类型
 * （2）Postgis中Geometry类型读取到SparkSQL中为StringType，值为wkb
 * TODO：
 * （1）利用 sessionInitStatement 改变时间戳类型的默认格式 https://blog.csdn.net/danengbinggan33/article/details/102852691
 * （2）分区字段的选择 https://blog.csdn.net/danengbinggan33/article/details/102852691
 * （3）测试 timestamp 类型的数据读取
 **/
@ToString
@Slf4j
public class PgLayerReader <T extends Layer> extends LayerReader<T> {

  @Getter
  @Setter
  protected PgLayerReaderConfig readerConfig;

  public PgLayerReader(SparkSession ss, LayerType layerType) {
    super(ss, layerType);
  }

  public PgLayerReader(SparkSession ss, PgLayerReaderConfig config) {
    super(ss, config.getLayerType());
    this.readerConfig = config;
  }

  @Override
  public T read() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {

    // TODO 封装 read procedure，去掉以下重复的图层读取参数设置
    checkReaderConfig();

    Dataset<Row> df = readFromSource();

    log.info("check field meta info");
    checkFields(df.schema());
    log.info("field meda info check successful");

    log.info("select targeted fields");
    Field[] fields = this.readerConfig.getAllAttributes();
    List<String> names = new ArrayList<>();
    for (Field field: fields) {
      if (field.isExist()) {
        names.add(field.getName());
      }
    }

    log.info("select column names: " + String.join(", ", names));
    if (names.size() == 1) {
      df = df.select(names.get(0));
    } else {
      String first = names.get(0);
      names.remove(0);
      String[] tnames = new String[names.size()];
      names.toArray(tnames);
      df = df.select(first, tnames);
    }

    log.info("data schema: \n" + df.schema().treeString());

    log.info(String.format("Transform Typed DataFrame to GISSpark %s Layer", this.readerConfig.getLayerType().name()));
    JavaRDD<Row> row = df.toJavaRDD();
    JavaPairRDD<String, Feature> features = row.mapToPair(new PairFunction<Row, String, Feature>() {
      @Override
      public Tuple2<String, Feature> call(Row row) throws Exception {

        // set up feature id
        Field idf = readerConfig.getIdField();
        String fid;
        if (idf.isExist()) {
          fid = String.valueOf(row.get(row.fieldIndex(idf.getName())));
        } else {
          fid = UUID.randomUUID().toString();
        }

        // set up geometry
        Field geof = readerConfig.getShapeField();
        String wkbstr = row.getString(row.fieldIndex(geof.getName()));
        WKBReader wkbReader = new WKBReader();
        byte[] aux = WKBReader.hexToBytes(wkbstr);
        Geometry geometry = wkbReader.read(aux);

        // set up timestamp if exists
        Long timestamp = null;
        Field tf = readerConfig.getTimeField();
        if (tf.getIndex() != Term.FIELD_NOT_EXIST) {
          timestamp = Long.valueOf(row.getString(row.fieldIndex(tf.getName())));
        }

        // set up starttime if exists
        Long startTime = null;
        Field stf = readerConfig.getStartTimeField();
        if (stf.isExist()) {
          startTime = Long.valueOf(row.getString(row.fieldIndex(stf.getName())));
        }

        // set up endtime if exists
        Long endTime = null;
        Field etf = readerConfig.getEndTimeField();
        if (etf.isExist()) {
          endTime = Long.valueOf(String.valueOf(row.get(row.fieldIndex(etf.getName()))));
        }

        // set feature attributes
        LinkedHashMap<Field, Object> attributes = new LinkedHashMap<>();
        Field[] fs = readerConfig.getAttributes();
        for (Field f: fs) {
          String name = f.getName();
          String cname = f.getType();
          Class c = Class.forName(cname);
          attributes.put(f, Converter.convert(String.valueOf(row.get(row.fieldIndex(name))), c));
        }

        Feature feature = buildFeature(readerConfig.getLayerType().getFeatureType(), fid, geometry, attributes, timestamp, startTime, endTime);
        return new Tuple2<>(feature.getFid(), feature);
      }
    });

    T layer = this.rddToLayer(features.rdd());
    LayerMetadata lm = new LayerMetadata();
    lm.setLayerId(readerConfig.getLayerId());
    lm.setCrs(readerConfig.getCrs());
    lm.setLayerName(readerConfig.getLayerName());

    // this.readerConfig.getIdField().setIndex(Term.FIELD_EXIST);
    lm.setAttributes(readerConfig.getAllAttributes());

    layer.setMetadata(lm);
    return layer;
  }

  protected Dataset<Row> readFromSource() {
    String dbtableSql = String.format("(%s) as %s_t", this.getReaderConfig().getFilterSql(), this.readerConfig.getDbtable());
    return this.ss.read().format("jdbc")
      .option("url", this.readerConfig.getSourcePath())
      .option("dbtable",  dbtableSql)
      .option("user", this.readerConfig.getUsername())
      .option("password", this.readerConfig.getPassword())
      .option("continueBatchOnError",true)
      .option("pushDownPredicate", true) // 默认请求下推
      .load();
  }

  protected void checkReaderConfig() {
    if (this.readerConfig == null) {
      throw new LayerReaderException("set layer reader config");
    }

    if (!this.readerConfig.check()) {
      throw new LayerReaderException("reader config is not set correctly");
    }
  }

  /**
   * 查看 dataframe 的 structtype 与 用户指定的 field 是否一致
   * @param st
   */
  protected void checkFields(StructType st) throws LayerReaderException {
    Field[] fs = this.readerConfig.getAllAttributes();
    StructField[] sfs = st.fields();
    boolean[] flags = new boolean[fs.length];
    for (int i=0; i<fs.length; i++) {
      Field f = fs[i];
      if (!f.isExist()) {
        flags[i] = true;
        continue;
      }
      flags[i] = false;
      for (StructField sf: sfs) {
        if (sf.name().equals(f.getName()) && Field.equalFieldClass(sf.dataType(), f.getType())) {
          flags[i] = true;
          break;
        }
      }
    }

    for (int m=0; m<flags.length; m++) {
      if (!flags[m]) {
        throw new LayerReaderException(String.format("Field %s not exists in table %s ", fs[m].toString(), this.readerConfig.getDbtable()));
      }
    }
  }

  @Override
  public void close() throws IOException {
    log.info(String.format("PgLayerReader close: %s", readerConfig.toString()));
  }

}
