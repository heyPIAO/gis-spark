package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Hu
 * @date 2020/9/15
 * 二调数据关联生成merge表
 **/
@Slf4j
public class LandFlowPreProcess extends BaseModel<LandFlowPreProcessArgs> {

    public LandFlowPreProcess(SparkSessionType type, String[] args) {
        super(type, args);
    }

    @Override
    protected void run() throws Exception {

        LayerReaderConfig lxdwConfig = LayerFactory.getReaderConfig(this.arg.getLxdwReaderConfig());
        LayerReaderConfig xzdwConfig = LayerFactory.getReaderConfig(this.arg.getXzdwReaderConfig());
        LayerReaderConfig dltbConfig = LayerFactory.getReaderConfig(this.arg.getDltbReaderConfig());

        LayerReader lxdwReader = LayerFactory.getReader(this.ss, lxdwConfig);
        LayerReader xzdwReader = LayerFactory.getReader(this.ss, xzdwConfig);
        LayerReader dltbReader = LayerFactory.getReader(this.ss, dltbConfig);

        Layer lxdwLayer = lxdwReader.read();
        Layer xzdwLayer = xzdwReader.read();
        Layer dltbLayer = dltbReader.read();

        // 处理地类图斑层与现状地物层，连接字段为：zldwdm, tbbh=zltbbh
        JavaPairRDD<String, Feature> dltbLayer2 = dltbLayer.mapToPair(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> o) throws Exception {
                String[] kf = new String[2];
                kf[0] = o._2.getAttribute("ZLDWDM").toString();
                kf[1] = o._2.getAttribute("TBBH").toString();
                return new Tuple2<>(StringUtils.join(kf, "##"), o._2);
            }
        });


        JavaPairRDD<String, Feature> lxdwLayer2 = lxdwLayer.mapToPair(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> o) throws Exception {
                String[] kf = new String[2];
                kf[0] = o._2.getAttribute("ZLDWDM").toString();
                kf[1] = o._2.getAttribute("ZLTBBH").toString();
                return new Tuple2<>(StringUtils.join(kf, "##"), o._2);
            }
        });

        JavaPairRDD<String, Tuple2<Feature, Optional<Feature>>> tl1 = dltbLayer2.leftOuterJoin(lxdwLayer2);

        JavaPairRDD<String, Feature> l1 = tl1.mapToPair(new PairFunction<Tuple2<String, Tuple2<Feature, Optional<Feature>>>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Tuple2<Feature, Optional<Feature>>> in) throws Exception {
                Feature f = new Feature(in._2._1);
                String[] keys = new String[2];
                keys[0] = f.getAttribute("ZLDWDM").toString();
                keys[1] = f.getAttribute("TBBH").toString();
                if (!in._2._2.isPresent())
                    return new Tuple2<>(StringUtils.join(keys, "##"), in._2._1);
                Feature lx = in._2._2.get();
                Field bsmF = new Field("lx_id");
                Field dlbmF = new Field("lx_dlbm");
                Field mjF = new Field("lx_mj");
                mjF.setType(Double.class);
                Field wktF = new Field("lx_wkt");
                f.addAttribute(bsmF, lx.getAttribute("BSM"));
                f.addAttribute(dlbmF, lx.getAttribute("DLBM"));
                f.addAttribute(mjF, lx.getAttribute("MJ"));
                f.addAttribute(wktF, lx.getAttribute("WKT"));
                return new Tuple2<>(StringUtils.join(keys, "##"), f);
            }
        });

        JavaPairRDD<String, Feature> xzdwLayer1 = xzdwLayer.mapToPair(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> o) throws Exception {
                String[] kf = new String[2];
                kf[0] = o._2.getAttribute("KCTBDWDM1").toString();
                kf[1] = o._2.getAttribute("KCTBBH1").toString();
                return new Tuple2<>(StringUtils.join(kf, "##"), o._2);
            }
        });

        JavaPairRDD<String, Feature> xzdwLayer2 = xzdwLayer.mapToLayer(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> o) throws Exception {
                String[] kf = new String[2];
                kf[0] = o._2.getAttribute("KCTBDWDM2").toString();
                kf[1] = o._2.getAttribute("KCTBBH2").toString();
                return new Tuple2<>(StringUtils.join(kf, "##"), o._2);
            }
        });

        JavaPairRDD<String, Tuple2<Feature, Optional<Feature>>> dltb_xzdw_1 = l1.leftOuterJoin(xzdwLayer1);

        JavaPairRDD<String, Feature> l2 = dltb_xzdw_1.mapToPair(new PairFunction<Tuple2<String, Tuple2<Feature, Optional<Feature>>>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Tuple2<Feature, Optional<Feature>>> in) throws Exception {
                Feature f = new Feature(in._2._1);
                if (!in._2._2.isPresent()) return new Tuple2<>(in._1, in._2._1);
                Feature xz = in._2._2.get();
                Field bsmF = new Field("xz_id");
                Field dlbmF = new Field("xz_dlbm");
                Field cdF = new Field("xz_cd");
                cdF.setType(Double.class);
                Field kdF = new Field("xz_kd");
                kdF.setType(Double.class);
                Field wktF = new Field("xz_wkt");
                Field kcblF = new Field("xz_kcbl");
                kcblF.setType(Double.class);
                f.addAttribute(bsmF, xz.getAttribute("BSM"));
                f.addAttribute(dlbmF, xz.getAttribute("DLBM"));
                f.addAttribute(cdF, xz.getAttribute("CD"));
                f.addAttribute(kdF, xz.getAttribute("KD"));
                f.addAttribute(wktF, xz.getAttribute("WKT"));
                f.addAttribute(kcblF, xz.getAttribute("KCBL"));

                Feature lx = in._2._1;
                Field lx_id = new Field("lx_id");
                Field lx_dlbm = new Field("lx_dlbm");
                Field lx_mj = new Field("lx_mj");
                lx_mj.setType(Double.class);
                Field lx_wkt = new Field("lx_wkt");
                f.addAttribute(lx_id, lx.getAttribute("BSM"));
                f.addAttribute(lx_dlbm, lx.getAttribute("DLBM"));
                f.addAttribute(lx_mj, lx.getAttribute("MJ"));
                f.addAttribute(lx_wkt, lx.getAttribute("WKT"));

                Field tb_wkt = new Field("tb_wkt");
                f.addAttribute(tb_wkt, in._2._1.getGeometryWkt());

                return new Tuple2<>(in._1, f);
            }
        });

        JavaPairRDD<String, Tuple2<Feature, Optional<Feature>>> dltb_xzdw_2 = l2.leftOuterJoin(xzdwLayer2);

        JavaPairRDD<String, Feature> l3 = dltb_xzdw_2.mapToPair(new PairFunction<Tuple2<String, Tuple2<Feature, Optional<Feature>>>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Tuple2<Feature, Optional<Feature>>> in) throws Exception {
                Feature f = new Feature(in._2._1);
                if (!in._2._2.isPresent())
                    return new Tuple2<>(in._1, in._2._1);
                if (in._2._1.getAttribute("xz_id") != null) {
                    return new Tuple2<>("EMPTY", null);
                }
                Feature xz = in._2._2.get();
                Field bsmF = new Field("xz_id");
                Field dlbmF = new Field("xz_dlbm");
                Field cdF = new Field("xz_cd");
                cdF.setType(Double.class);
                Field kdF = new Field("xz_kd");
                kdF.setType(Double.class);
                Field wktF = new Field("xz_wkt");
                Field kcblF = new Field("xz_kcbl");
                kcblF.setType(Double.class);
                f.addAttribute(bsmF, xz.getAttribute("BSM"));
                f.addAttribute(dlbmF, xz.getAttribute("DLBM"));
                f.addAttribute(cdF, xz.getAttribute("CD"));
                f.addAttribute(kdF, xz.getAttribute("KD"));
                f.addAttribute(wktF, xz.getAttribute("WKT"));
                f.addAttribute(kcblF, xz.getAttribute("KCBL"));

                Feature lx = in._2._1;
                Field lx_id = new Field("lx_id");
                Field lx_dlbm = new Field("lx_dlbm");
                Field lx_mj = new Field("lx_mj");
                lx_mj.setType(Double.class);
                Field lx_wkt = new Field("lx_wkt");
                f.addAttribute(lx_id, lx.getAttribute("BSM"));
                f.addAttribute(lx_dlbm, lx.getAttribute("DLBM"));
                f.addAttribute(lx_mj, lx.getAttribute("MJ"));
                f.addAttribute(lx_wkt, lx.getAttribute("WKT"));

                Field tb_wkt = new Field("tb_wkt");
                f.addAttribute(tb_wkt, in._2._1.getGeometryWkt());

                return new Tuple2<>(in._1, f);
            }
        }).filter(x -> !x._1.equals("EMPTY"));

        JavaPairRDD<String, Feature> unionLayer = l2.union(l3);

        unionLayer.cache();
        Feature f = unionLayer.first()._2;
        log.info("catch");

//        unionLayer.saveAsTextFile("/Users/moral/Desktop/result/");

        JavaRDD<Row> rdd = unionLayer.map(x -> x._2).map(x -> transform(x));
        StructType st = this.getLayerStructType();
        Dataset<Row> df = this.ss.createDataFrame(rdd, st);
        df.cache();
        df.printSchema();
        df.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql://10.79.231.83:5430/template_postgis")
                .option("dbtable", String.format("%s.%s", "public", "merge_spark"))
                .option("user", "postgres")
                .option("password", "postgres")
                .mode(SaveMode.Overwrite)
                .save();
        df.unpersist();

        log.info(String.valueOf(l1.count()));
    }
    private StructType getLayerStructType() {
        List<StructField> sfsl = new LinkedList<>();

        Field BSM = new Field("BSM");
        sfsl.add(convertFieldToStructField(BSM));
        Field ZLDWDM = new Field("ZLDWDM");
        sfsl.add(convertFieldToStructField(ZLDWDM));
        Field TBBH = new Field("TBBH");
        sfsl.add(convertFieldToStructField(TBBH));
        Field DLBM = new Field("DLBM");
        sfsl.add(convertFieldToStructField(DLBM));
        Field TBMJ = new Field("TBMJ");
        sfsl.add(convertFieldToStructField(TBMJ));
        Field TKXS = new Field("TKXS");
        sfsl.add(convertFieldToStructField(TKXS));
        Field tb_wkt = new Field("tb_wkt");
        sfsl.add(convertFieldToStructField(tb_wkt));

        Field xz_id = new Field("xz_id");
        sfsl.add(convertFieldToStructField(xz_id));
        Field xz_dlbm = new Field("xz_dlbm");
        sfsl.add(convertFieldToStructField(xz_dlbm));
        Field xz_cd = new Field("xz_cd");
//        xz_cd.setType(Double.class);
        sfsl.add(convertFieldToStructField(xz_cd));
        Field xz_kd = new Field("xz_kd");
//        xz_kd.setType(Double.class);
        sfsl.add(convertFieldToStructField(xz_kd));
        Field xz_wkt = new Field("xz_wkt");
        sfsl.add(convertFieldToStructField(xz_wkt));
        Field xz_kcbl = new Field("xz_kcbl");
//        xz_kcbl.setType(Double.class);
        sfsl.add(convertFieldToStructField(xz_kcbl));

        Field lx_id = new Field("lx_id");
        sfsl.add(convertFieldToStructField(lx_id));
        Field lx_dlbm = new Field("lx_dlbm");
        sfsl.add(convertFieldToStructField(lx_dlbm));
        Field lx_mj = new Field("lx_mj");
//        lx_mj.setType(Double.class);
        sfsl.add(convertFieldToStructField(lx_mj));
        Field lx_wkt = new Field("lx_wkt");
        sfsl.add(convertFieldToStructField(lx_wkt));

        StructField[] sfs = new StructField[sfsl.size()];
        sfsl.toArray(sfs);
        return new StructType(sfs);
    }

    public Row transform(Feature feature) {
        Row row = new GenericRow(feature.toObjectArray());
        return row;
    }

    private StructField convertFieldToStructField(Field field) {
        return new StructField(field.getName(),
                Field.converFieldTypeToDataType(field),
                !(field.getFieldType().equals(FieldType.ID_FIELD) || field.getFieldType().equals(FieldType.SHAPE_FIELD)),
                Metadata.empty());
    }

    public static void main(String[] args) throws Exception {
        LandFlowPreProcess preProcess = new LandFlowPreProcess(SparkSessionType.LOCAL, args);
        preProcess.exec();
    }

}
