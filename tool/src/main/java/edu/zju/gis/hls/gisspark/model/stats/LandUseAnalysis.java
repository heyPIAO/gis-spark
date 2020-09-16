package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2020/9/10
 * 土地利用现状分析模型
 * 亩 = 平方米 / 666.6666667
 * 平方千米 = 平方米 / 1000000.00
 **/
@Slf4j
public class LandUseAnalysis extends BaseModel<LandUseAnalysisArgs> {

    private static Integer DEFAULT_INDEX_LEVEL = 12;
    private static Integer DEFAULT_SCALE = 2;
    private static Double DEFAULT_MU = 666.6666667;
    private static Double DEFAULT_KM = 1000000.00;

    public LandUseAnalysis(SparkSessionType type, String[] args) {
        super(type, args);
    }

    /**
     * 范围图层字段不得包含三调地类数据的标准字段名
     *
     * @return
     */
    private boolean checkExtendLayerFieldValid(Field f) {
        String name = f.getName();
        return !(name.equals("YTBMJ") || name.equals("DLBM") || name.equals("BSM") || name.equals("KCDLBM") || name.equals("ZLDWDM")
                || name.equals("TBMJ") || name.equals("TBDLMJ") || name.equals("EMPTY")) || name.equals("KZMJ");
    }

    @Override
    protected void run() throws Exception {
        LayerReaderConfig extentLayerReaderConfig = LayerFactory.getReaderConfig(this.arg.getExtentReaderConfig());
        for (Field f : extentLayerReaderConfig.getAttributes()) {
            if (!checkExtendLayerFieldValid(f))
                throw new GISSparkException(String.format("Unvalid extend layer field %s, " +
                        "field name cannot be in [YTBMJ, DLBM, BSM, KCDLBM, ZLDWDM, TBMJ, TBDLMJ, KZMJ, EMPTY]", f.toString()));
        }

        LayerReader extendLayerReader = LayerFactory.getReader(this.ss, extentLayerReaderConfig);

        Layer extendLayer = extendLayerReader.read();
        extendLayer.makeSureCached();
        extendLayer.analyze();

        Geometry approximateExtendGeom = extendLayer.getMetadata().getGeometry();

        // 根据用户指定字段获得控制面积 -- 平面面积
        Map<String, Feature> tareas = extendLayer.mapToLayer(new Function<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
                List<String> extendFieldStr = Arrays.stream(extentLayerReaderConfig.getAttributes()).map(x->x.getName()).collect(Collectors.toList());
                Field kzmj = new Field("KZMJ");
                kzmj.setType(Double.class);
                input._2.addAttribute(kzmj, input._2.getGeometry().getArea());
                return new Tuple2<String, Feature>(StringUtils.join(extendFieldStr, "##"), input._2);
            }
        }).groupByKey().reduceByKey(new Function2<Feature, Feature, Feature> () {
            @Override
            public Feature call(Feature f1, Feature f2) throws Exception {
                Feature f = new Feature(f1);
                f.updateAttribute("KZMJ", (Double)f1.getAttribute("KZMJ")+(Double)f2.getAttribute("KZMJ"));
                return f;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Feature>, String, Feature>(){
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> v1) throws Exception {
                Double area = (Double) v1._2.getAttribute("KZMJ");
                Double m = area / DEFAULT_MU;
                Double km2 = area / DEFAULT_KM;
                Field mf = new Field("KZMJ_M");
                mf.setType(Double.class);
                Field kmf = new Field("KZMJ_KM");
                kmf.setType(Double.class);
                Feature f = new Feature(v1._2);
                f.addAttribute(mf, m);
                f.addAttribute(kmf, km2);
                return new Tuple2<>(v1._1, f);
            }
        }).collectAsMap();

        LayerReaderConfig targetLayerReaderConfig = LayerFactory.getReaderConfig(this.arg.getTargetReaderConfig());
        SourceType st = SourceType.getSourceType(targetLayerReaderConfig.getSourcePath());
        LayerReader targetLayerReader = null;
        if (st.equals(SourceType.PG) || st.equals(SourceType.CitusPG)) {
            // 基于范围图斑构造空间查询语句
            String extentWKT = approximateExtendGeom.toText();
            String filterSql = String.format("st_intersects(st_geomFromWKT(%s),st_geomfromtext(\"WKT\",%d))", extentWKT, CRS.lookupEpsgCode(targetLayerReaderConfig.getCrs(), false));
            PgLayerReaderConfig pgLayerReaderConfig = (PgLayerReaderConfig) targetLayerReaderConfig;
            pgLayerReaderConfig.setFilter(filterSql);
            targetLayerReader = LayerFactory.getReader(this.ss, pgLayerReaderConfig);
        } else {
            targetLayerReader = LayerFactory.getReader(this.ss, targetLayerReaderConfig);
        }
        Layer targetLayer = targetLayerReader.read().mapToLayer(new Function<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> v1) throws Exception {
                Feature of = new Feature(v1._2);
                Field oarea = new Field("YTBMJ");
                oarea.setType(Double.class);
                of.addAttribute(oarea, v1._2.getGeometry().getArea());
                return new Tuple2<>(v1._1, of);
            }
        });

        DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID,
                new UniformGridIndexConfig(DEFAULT_INDEX_LEVEL, false));
        KeyIndexedLayer indexedLayer = si.index(targetLayer);
        KeyIndexedLayer extendIndexedLayer = si.index(extendLayer);

        Layer filteredLayer = indexedLayer.intersect(extendIndexedLayer, true).toLayer();

        Layer intersectedLayer = filteredLayer.mapToLayer(new Function<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
                Feature feature = input._2;
                Geometry geometry = feature.getGeometry();
                Feature fo = new Feature(feature); //深拷贝
//                String dlbm = feature.getAttribute("DLBM_1").toString();
//                String bsm = feature.getAttribute("BSM_1").toString();
//                String kcdlbm = feature.getAttribute("KCDLBM_1").toString();
//                String zldwdm = feature.getAttribute("ZLDWDM_1").toString();
                Double tbmj = Double.valueOf(feature.getAttribute("TBMJ_1").toString());
                Double tbdlmj = Double.valueOf(feature.getAttribute("TBDLMJ_1").toString());
                Double xtbmj = geometry.getArea();
                Double ytbmj = Double.valueOf(feature.getAttribute("YTBMJ_1").toString());

                Double area = tbdlmj * xtbmj / ytbmj;
                Double xarea = tbmj * xtbmj / ytbmj;

                fo.updateAttribute("TBDLMJ_1", area);
                fo.updateAttribute("TBMJ_1", xarea);
//                LinkedHashMap<Field, Object> fields = new LinkedHashMap<>();
//                fields.put(new Field("DLBM"), dlbm);
//                fields.put(new Field("KCDLBM"), kcdlbm);
//                fields.put(new Field("ZLDWDM"), zldwdm);
//                fields.put(new Field("TBDLMJ"), area);
//                fields.put(new Field("TBMJ"), xarea);

//                Field[] extendFields = extentLayerReaderConfig.getAttributes();
//                for (Field f : extendFields) {
//                    String fname = f.getName() + "_2";
//                    fields.put(f, feature.getAttribute(fname));
//                }
//
//                Feature fo = new Feature(feature);//深拷贝
//                fo.setAttributes(fields);
                return new Tuple2<>(input._1, fo);
            }
        }).distinct();

        intersectedLayer.cache();

        Layer kcLayer = ((Layer<String, Feature>) intersectedLayer).mapToLayer(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
                Feature feature = input._2;
                Feature of = new Feature(feature);
//                String dlbm = feature.getAttribute("DLBM").toString();
                String kcdlbm = feature.getAttribute("KCDLBM_1").toString();
//                String zldwdm = feature.getAttribute("ZLDWDM_1").toString();
                Double tbmj = Double.valueOf(feature.getAttribute("TBMJ_1").toString());
                Double tbdlmj = Double.valueOf(feature.getAttribute("TBDLMJ_1").toString());
                if (!kcdlbm.equals(null) && kcdlbm.length() > 3) {
                    LinkedHashMap<Field, Object> fields = new LinkedHashMap<>();
                    of.updateAttribute("DLBM_1", kcdlbm);
//                    fields.put(new Field("DLBM"), kcdlbm);
//                    fields.put(new Field("ZLDWDM"), zldwdm);
                    of.updateAttribute("TBDLMJ_1", tbmj-tbdlmj);
//                    fields.put(new Field("TBDLMJ"), tbmj - tbdlmj);

//                    Field[] extendFields = extentLayerReaderConfig.getAttributes();
//                    for (Field f : extendFields) {
//                        String fname = f.getName();
//                        fields.put(f, feature.getAttribute(fname));
//                    }
//
//                    Feature f = new Feature(feature);
//                    f.setAttributes(fields);

                    return new Tuple2<>(input._1, of);
                }
                return new Tuple2<>("EMPTY", null);
            }
        }).filterToLayer(new Function<Tuple2<String, Feature>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Feature> input) throws Exception {
                return !input._1.equals("EMPTY");
            }
        });

        Layer layer = intersectedLayer.union(kcLayer);
        intersectedLayer.unpersist();

        Layer resultLayer = layer.mapToLayer(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
                Field[] extendFields = extentLayerReaderConfig.getAttributes();
                String[] key = new String[extendFields.length + 2];
                key[0] = input._2.getAttribute("DLBM_1").toString();
                key[1] = input._2.getAttribute("ZLDWDM_1").toString();
                for (int i = 2; i < key.length; i++) {
                    key[i] = input._2.getAttribute(extendFields[i - 2]).toString();
                }
//                Feature f = new Feature(input._2);
                return new Tuple2<>(StringUtils.join(key, "##"), input._2);
            }
        });

        resultLayer.cache();

        // 写出裁切以后面积调整过的图层
        LayerWriterConfig geomWriterConfig = LayerFactory.getWriterConfig(this.arg.getGeomWriterConfig());
        LayerWriter geomWriter = LayerFactory.getWriter(this.ss, geomWriterConfig);
        geomWriter.write(resultLayer);

        JavaPairRDD<String, Iterator<Feature>> oareaRDD = resultLayer.reduceToLayer(new Function2<Feature, Feature, Feature>() {
            @Override
            public Feature call(Feature input1, Feature input2) throws Exception {
                Double tbdlmj1 = Double.valueOf(input1.getAttribute("TBDLMJ_1").toString());
                Double tbdlmj2 = Double.valueOf(input2.getAttribute("TBDLMJ_1").toString());
                Double tbdlmj = tbdlmj1 + tbdlmj2;
                Feature f = new Feature(input1);
                f.updateAttribute("TBDLMJ_1", tbdlmj);
//                LinkedHashMap<Field, Object> attr = f.getAttributes();
//                for (Field key : attr.keySet()) {
//                    if (key.getName().equals("TBDLMJ")) {
//                        attr.put(key, tbdlmj);
//                    }
//                }
                return f;
            }
        }).mapToLayer(new Function<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> v1) throws Exception {
                List<String> extendFieldStr = Arrays.stream(extentLayerReaderConfig.getAttributes()).map(x->x.getName()).collect(Collectors.toList());
                return new Tuple2<>(StringUtils.join(extendFieldStr, "##"), v1._2);
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<String, Iterator<Feature>>, String, Iterator<Feature>> () {
            @Override
            public Tuple2<String, Iterator<Feature>> call(Tuple2<String, Iterator<Feature>> v1) throws Exception {
                List<Feature> features = IteratorUtils.toList(v1._2);
                Double tarea = (Double) tareas.get(v1._1).getAttribute("KZMJ_1");
                return new Tuple2<>(v1._1, AreaAdjustment.adjust(tarea, "KZMJ_1", features, DEFAULT_SCALE).iterator());
            }
        });

        JavaPairRDD<String, Feature> resultRDD = oareaRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterator<Feature>>, String, Feature>() {
            @Override
            public Iterator<Tuple2<String, Feature>> call(Tuple2<String, Iterator<Feature>> input) throws Exception {
                List<Feature> features = IteratorUtils.toList(input._2);
                return features.stream().map(x->new Tuple2<>(x.getFid(), x)).collect(Collectors.toList()).iterator();
            }
        });

        resultRDD.cache();

        Layer result = new Layer(resultRDD.rdd());
        String layername = result.getMetadata().getLayerName();
        LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getStatsWriterConfig());
        LayerWriter resultWriter = LayerFactory.getWriter(this.ss, writerConfig);
        resultWriter.write(result);

        // 转换到亩，控平差并输出
        JavaPairRDD<String, Feature> resultRDDM = resultRDD.mapToPair(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> in) throws Exception {
                Feature f = new Feature(in._2);
                Double area = (Double) f.getAttribute("KZMJ_1");
                Double m = area / DEFAULT_MU;
                f.updateAttribute("KZMJ_1", m);
                return new Tuple2<String, Feature>(in._1, f);
            }
        });
        Layer resultM = new Layer(resultRDDM.rdd());
        layer.setName(layername + "_mu");
        resultWriter.write(resultM);

        // 转换到平方千米，控平差并输出
        JavaPairRDD<String, Feature> resultRDDKM = resultRDD.mapToPair(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> in) throws Exception {
                Feature f = new Feature(in._2);
                Double area = (Double) f.getAttribute("KZMJ_1");
                Double km2 = area / DEFAULT_KM;
                f.updateAttribute("KZMJ_1", km2);
                return new Tuple2<String, Feature>(in._1, f);
            }
        });

        Layer resultKM = new Layer(resultRDDKM.rdd());
        layer.setName(layername + "_km");
        resultWriter.write(resultKM);
    }


    public static void main(String[] args) throws Exception {
        LandUseAnalysis analysis = new LandUseAnalysis(SparkSessionType.LOCAL, args);
        analysis.exec();
    }

}
