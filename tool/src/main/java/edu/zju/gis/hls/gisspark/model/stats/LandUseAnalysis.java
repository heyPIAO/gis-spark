package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerMetadata;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.SourceType;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.pg.PgLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.*;

import static tec.uom.se.function.QuantityFunctions.sum;

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
    private static Double DEFAULT_KM = 10000.00;//0921改为公顷

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
            public Tuple2<String, Feature> call(Tuple2<String, Feature> o) throws Exception {
                Feature of = new Feature(o._2);
                of.setFid(String.format("%s.%s", o._2.getFid(), o._2.getAttribute(Term.FIELD_DEFAULT_ID.getName() + "_2").toString()));
                return new Tuple2<>(of.getFid(), of);
            }
        }).reduceByKeyToLayer(new Function2<Feature, Feature, Feature>() {
            @Override
            public Feature call(Feature o, Feature o2) throws Exception {
                return o;
            }
        }).mapToLayer(new Function<Tuple2<String, Feature>, Tuple2<String, Feature>>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
                Feature feature = input._2;
                Geometry geometry = feature.getGeometry();
                Feature fo = new Feature(feature); //深拷贝
                Double tbmj = Double.valueOf(feature.getAttribute("TBMJ_1").toString());
                Double kcxs = Double.valueOf(feature.getAttribute("KCXS_1").toString());
                Double tbdlmj = Double.valueOf(feature.getAttribute("TBDLMJ_1").toString());
                Double xtbmj = geometry.getArea();
                Double ytbmj = Double.valueOf(feature.getAttribute("YTBMJ_1").toString());
//                String bsm=feature.getAttribute("BSM_1").toString();
//                String fid=feature.getAttribute("fid_2").toString();
                String zldwdm=feature.getAttribute("ZLDWDM_1").toString().substring(0,6);

                Double area = tbdlmj * xtbmj / ytbmj;
                Double xarea = tbmj * xtbmj / ytbmj;

                Field pmmjF = new Field("PMMJ");
                pmmjF.setType(Double.class);

                Field ypmmjF = new Field("YPMMJ");
                ypmmjF.setType(Double.class);

                fo.updateAttribute("TBDLMJ_1", area);
                fo.updateAttribute("TBMJ_1", xarea);
                fo.updateAttribute("ZLDWDM_1", zldwdm);
                fo.addAttribute(pmmjF, xtbmj*(1-kcxs));
                fo.addAttribute(ypmmjF, xtbmj);

                return new Tuple2<>(input._1, fo);
            }
        });

        intersectedLayer.cache();

        Layer kcLayer = ((Layer<String, Feature>) intersectedLayer).mapToLayer(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
                Feature feature = input._2;
                Feature of = new Feature(feature);
                String kcdlbm = feature.getAttribute("KCDLBM_1").toString();
                Double tbmj = Double.valueOf(feature.getAttribute("TBMJ_1").toString());
                Double tbdlmj = Double.valueOf(feature.getAttribute("TBDLMJ_1").toString());
                Double ypmmj = Double.valueOf(feature.getAttribute("YPMMJ").toString());
                Double pmmj = Double.valueOf(feature.getAttribute("PMMJ").toString());
                if (!kcdlbm.equals(null) && kcdlbm.length() > 3) {
                    LinkedHashMap<Field, Object> fields = new LinkedHashMap<>();
                    of.updateAttribute("DLBM_1", kcdlbm);
                    of.updateAttribute("TBDLMJ_1", tbmj-tbdlmj);
                    of.updateAttribute("PMMJ", ypmmj-pmmj);

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

        layer.cache();

        // 写出裁切以后面积调整过的图层
//        LayerWriterConfig geomWriterConfig = LayerFactory.getWriterConfig(this.arg.getGeomWriterConfig());
//        LayerWriter geomWriter = LayerFactory.getWriter(this.ss, geomWriterConfig);
//        layer.inferFieldMetadata();
//        geomWriter.write(layer);

        Layer resultLayer = layer.mapToLayer(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
                Field[] extendFields = extentLayerReaderConfig.getAttributes();
                String[] key = new String[extendFields.length + 2];
                key[0] = input._2.getAttribute("DLBM_1").toString();
                key[1] = input._2.getAttribute("ZLDWDM_1").toString().substring(0,6);
                for (int i = 2; i < key.length; i++) {
                    key[i] = input._2.getAttribute(extendFields[i - 2].getName() + "_2").toString();
                }
                return new Tuple2<>(StringUtils.join(key, "##"), input._2);
            }
        });

        JavaPairRDD<String, Feature> oareaRDD = resultLayer.reduceByKeyToLayer(new Function2<Feature, Feature, Feature>() {
            @Override
            public Feature call(Feature input1, Feature input2) throws Exception {
                Double tbdlmj1 = Double.valueOf(input1.getAttribute("TBDLMJ_1").toString());
                Double tbdlmj2 = Double.valueOf(input2.getAttribute("TBDLMJ_1").toString());
                Double tbdlmj = tbdlmj1 + tbdlmj2;

                Double pmmj1 = Double.valueOf(input1.getAttribute("PMMJ").toString());
                Double pmmj2 = Double.valueOf(input2.getAttribute("PMMJ").toString());
                Double pmmj = pmmj1 + pmmj2;
                Feature f = new Feature(input1);
                f.updateAttribute("TBDLMJ_1", tbdlmj);
                f.updateAttribute("PMMJ", pmmj);
                return f;
            }
        });

        oareaRDD.cache();

        Feature totalF = oareaRDD.reduce(new Function2<Tuple2<String, Feature>, Tuple2<String, Feature>, Tuple2<String, Feature>>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> in1, Tuple2<String, Feature> in2) throws Exception {
                Feature f1 = in1._2;
                Feature f2 = in2._2;
                Double tbdlmj1 = Double.valueOf(f1.getAttribute("TBDLMJ_1").toString());
                Double tbdlmj2 = Double.valueOf(f2.getAttribute("TBDLMJ_1").toString());
                Double tbdlmj = tbdlmj1 + tbdlmj2;

                Double pmmj1 = Double.valueOf(f1.getAttribute("PMMJ").toString());
                Double pmmj2 = Double.valueOf(f2.getAttribute("PMMJ").toString());
                Double pmmj = pmmj1 + pmmj2;
                Feature f = new Feature(f1);
                f.updateAttribute("TBDLMJ_1", tbdlmj);
                f.updateAttribute("PMMJ", pmmj);
                return new Tuple2<>(in1._1(), f);
            }
        })._2;
        Double totalArea = Double.valueOf(totalF.getAttribute("TBDLMJ_1").toString());
        Double totalPmmj = Double.valueOf(totalF.getAttribute("PMMJ").toString());
        Double totalAreaMu = totalArea/DEFAULT_MU;
        Double totalAreaKm = totalArea/DEFAULT_KM;
        Double totalPmmjMu = totalPmmj/DEFAULT_MU;
        Double totalPmmjKm = totalPmmj/DEFAULT_KM;

        JavaRDD<Feature> adjustRDD = oareaRDD.map(new Function<Tuple2<String, Feature>, Feature>() {
            @Override
            public Feature call(Tuple2<String, Feature> in) throws Exception {
                Feature f = new Feature(in._2);
                Double tbdlmj = Double.valueOf(f.getAttribute("TBDLMJ_1").toString());
                Double tbdlmjMu = tbdlmj/DEFAULT_MU;
                Double tbdlmjKm = tbdlmj/DEFAULT_KM;
                Double pmmj = Double.valueOf(f.getAttribute("PMMJ").toString());
                Double pmmjMu = pmmj/DEFAULT_MU;
                Double pmmjKm = pmmj/DEFAULT_KM;

                Field mf = new Field("TBDLMJ_M");
                mf.setType(Double.class);
                f.addAttribute(mf, tbdlmjMu);
                Field kmf = new Field("TBDLMJ_KM");
                kmf.setType(Double.class);
                f.addAttribute(kmf, tbdlmjKm);

                Field pmf = new Field("PMMJ_M");
                pmf.setType(Double.class);
                f.addAttribute(pmf, pmmjMu);
                Field kpmf = new Field("PMMJ_KM");
                kpmf.setType(Double.class);
                f.addAttribute(kpmf, pmmjKm);

                return f;
            }
        });

        List<Feature> adjustFeatures = adjustRDD.collect();
        adjustFeatures = AreaAdjustment.adjust(totalAreaMu, "TBDLMJ_M", adjustFeatures, DEFAULT_SCALE);
        adjustFeatures = AreaAdjustment.adjust(totalAreaKm, "TBDLMJ_KM", adjustFeatures, DEFAULT_SCALE);
        adjustFeatures = AreaAdjustment.adjust(totalPmmjMu, "PMMJ_M", adjustFeatures, DEFAULT_SCALE);
        adjustFeatures = AreaAdjustment.adjust(totalPmmjKm, "PMMJ_KM", adjustFeatures, DEFAULT_SCALE);

        JavaPairRDD<String, Feature> resultRDD = this.jsc.parallelize(adjustFeatures).mapToPair(x->new Tuple2<>(x.getFid(), x));

        JavaPairRDD<String,Feature> reRDD=resultRDD.mapToPair(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
            @Override
            public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
                Feature feature=input._2;
                LinkedHashMap<Field,Object> fields=feature.getAttributes();
                LinkedHashMap<Field,Object> fieldsNew=new LinkedHashMap<>();
                Iterator<Field> iterator = fields.keySet().iterator();
                while (iterator.hasNext()) {
                    Field field = iterator.next();

                    Object value=fields.get(field);
                    if(field.getName().equals("TBDLMJ_1")){
                        Field fo=new Field("TQMJ");
                        fo.setType(Double.class);
                        fieldsNew.put(fo,value);
            }else if(field.getName().equals("ZLDWDM_1")){

                        Field fo=new Field("ZLDWDM");
                        fo.setType(String.class);
                        fieldsNew.put(fo,value);
            }else if(field.getName().equals("DLBM_1")){

                        Field fo=new Field("DLBM");
                        fo.setType(String.class);
                        fieldsNew.put(fo,value);
            }else if(field.getName().equals("TBDLMJ_M")){

                        Field fo=new Field("TQMJMU");
                        fo.setType(Double.class);
                        fieldsNew.put(fo,value);
            }else if(field.getName().equals("TBDLMJ_KM")){

                        Field fo=new Field("TQMJGQ");
                        fo.setType(Double.class);
                        fieldsNew.put(fo,value);
            }else if(field.getName().equals("PMMJ_M")){

                        Field fo=new Field("PMMJMU");
                        fo.setType(Double.class);
                        fieldsNew.put(fo,value);
            }
            else if(field.getName().equals("PMMJ_KM")){
                        Field fo=new Field("PMMJGQ");
                        fo.setType(Double.class);
                        fieldsNew.put(fo,value);

            }

        }
                feature.setAttributes(fieldsNew);
                return new Tuple2<>(input._1,feature);
            }
        });


        Layer result = new Layer(reRDD.rdd());

        LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getStatsWriterConfig());
        LayerWriter resultWriter = LayerFactory.getWriter(this.ss, writerConfig);
//        List<String> fieldNames=new ArrayList<>();
//        fieldNames.add("TBDLMJ_1");
//        fieldNames.add("ZLDWDM_1");
//        fieldNames.add("DLBM_1");
//        fieldNames.add("TBDLMJ_M");
//        fieldNames.add("TBDLMJ_KM");
//        fieldNames.add("PMMJ");
//        fieldNames.add("PMMJ_M");
//        fieldNames.add("PMMJ_KM");
//        Field[] extendFields = extentLayerReaderConfig.getAttributes();
//        for (int i=0;i<extendFields.length;i++){
//            fieldNames.add(extendFields[i].getName() + "_2");
//        }
        result.inferFieldMetadata();
//        LayerMetadata layerMetadata=result.getMetadata();
//        LinkedHashMap<Field,Object> re=new LinkedHashMap();
//
//        Iterator<Field> fields = layerMetadata.getAttributes().keySet().iterator();
//        while (fields.hasNext()) {
//            Field field=fields.next();
//
//            Object value=layerMetadata.getAttribute(field);
//
//            if(field.getName()=="TBDLMJ_1"){
//                field.setName("TQMJ");
//                field.setType(Double.class);
//            }else if(field.getName()=="ZLDWDM_1"){
//                field.setName("ZLDWDM");
//                field.setType(String.class);
//            }else if(field.getName()=="DLBM_1"){
//                field.setName("DLBM");
//                field.setType(String.class);
//            }else if(field.getName()=="TBDLMJ_M"){
//                field.setName("TQMJMU");
//                field.setType(Double.class);
//            }else if(field.getName()=="TBDLMJ_KM"){
//                field.setName("TQMJGQ");
//                field.setType(Double.class);
//            }else if(field.getName()=="PMMJ_M"){
//                field.setName("PMMJMU");
//                field.setType(Double.class);
//            }
//            else if(field.getName()=="PMMJ_KM"){
//                field.setName("PMMJKM");
//                field.setType(Double.class);
//
//            }
//            re.put(field,value);
//        }
//
//        layerMetadata.setAttributes(re);
//        result.setMetadata(layerMetadata);
        resultWriter.write(result);
    }


    public static void main(String[] args) throws Exception {
        LandUseAnalysis analysis = new LandUseAnalysis(SparkSessionType.SHELL, args);
        analysis.exec();
    }

}
