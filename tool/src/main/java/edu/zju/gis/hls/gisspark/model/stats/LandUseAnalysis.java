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
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2020/9/10
 * 土地利用现状分析模型
 **/
@Slf4j
public class LandUseAnalysis extends BaseModel<LandUseAnalysisArgs> {

  private static Integer DEFAULT_INDEX_LEVEL = 12;

  public LandUseAnalysis(SparkSessionType type, String[] args) {
    super(type, args);
  }

  /**
   * 范围图层字段不得包含三调地类数据的标准字段名
   * @return
   */
  private boolean checkExtendLayerFieldValid(Field f) {
    String name = f.getName();
    return !(name.equals("YTBMJ") || name.equals("DLBM") || name.equals("BSM") || name.equals("KCDLBM") || name.equals("ZLDWDM")
      || name.equals("TBMJ") || name.equals("TBDLMJ") || name.equals("EMPTY"));
  }

  @Override
  protected void run() throws Exception {
    LayerReaderConfig extentLayerReaderConfig=LayerFactory.getReaderConfig(this.arg.getExtentReaderConfig());
    for (Field f: extentLayerReaderConfig.getAttributes()) {
      if (!checkExtendLayerFieldValid(f))
        throw new GISSparkException(String.format("Unvalid extend layer field %s, " +
          "field name cannot be in [YTBMJ, DLBM, BSM, KCDLBM, ZLDWDM, TBMJ, TBDLMJ, EMPTY]", f.toString()));
    }

    LayerReader extendLayerReader = LayerFactory.getReader(this.ss, extentLayerReaderConfig);

    Layer extendLayer = extendLayerReader.read();
    extendLayer.makeSureCached();
    extendLayer.analyze();

    Geometry approximateExtendGeom = extendLayer.getMetadata().getGeometry();

    LayerReaderConfig targetLayerReaderConfig=LayerFactory.getReaderConfig(this.arg.getTargetReaderConfig());
    SourceType st = SourceType.getSourceType(targetLayerReaderConfig.getSourcePath());
    if (st.equals(SourceType.PG) || st.equals(SourceType.CitusPG)) {
      // 基于范围图斑构造空间查询语句
      String sql = String.format("");
    }

    LayerReader targetLayerReader = LayerFactory.getReader(this.ss, targetLayerReaderConfig);
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
        Feature feature=input._2;
        Geometry geometry=feature.getGeometry();
        String dlbm=feature.getAttribute("DLBM_1").toString();
        String bsm=feature.getAttribute("BSM_1").toString();
        String kcdlbm=feature.getAttribute("KCDLBM_1").toString();
        String zldwdm=feature.getAttribute("ZLDWDM_1").toString();
        Double tbmj=Double.valueOf(feature.getAttribute("TBMJ_1").toString());

        Double tbdlmj=Double.valueOf(feature.getAttribute("TBDLMJ_1").toString());
        Double xtbmj=geometry.getArea();
        Double ytbmj=Double.valueOf(feature.getAttribute("YTBMJ_1").toString());

        Double area=tbdlmj*xtbmj/ytbmj;
        Double xarea=tbmj*xtbmj/ytbmj;
        LinkedHashMap<Field,Object> fields=new LinkedHashMap<>();
        fields.put(new Field("DLBM"),dlbm);
        fields.put(new Field("KCDLBM"),kcdlbm);
        fields.put(new Field("ZLDWDM"),zldwdm);
        fields.put(new Field("TBDLMJ"),area);
        fields.put(new Field("TBMJ"),xarea);

        Field[] extendFields = extentLayerReaderConfig.getAttributes();
        for (Field f: extendFields) {
          String fname = f.getName() + "_2";
          fields.put(f, feature.getAttribute(fname));
        }

        Feature fo = new Feature(feature);//深拷贝
        fo.setAttributes(fields);
        return new Tuple2<>(input._1, fo);
      }
    }).distinct();


    Layer kcLayer=((Layer<String, Feature>) intersectedLayer).mapToLayer(new PairFunction<Tuple2<String, Feature>, String, Feature>() {
      @Override
      public Tuple2<String, Feature> call(Tuple2<String, Feature> input) throws Exception {
        Feature feature=input._2;
        String dlbm=feature.getAttribute("DLBM").toString();
        String kcdlbm=feature.getAttribute("KCDLBM").toString();
        String zldwdm=feature.getAttribute("ZLDWDM").toString();
        Double tbmj=Double.valueOf(feature.getAttribute("TBMJ").toString());
        Double tbdlmj=Double.valueOf(feature.getAttribute("TBDLMJ").toString());
        if(!kcdlbm.equals(null)&&kcdlbm.length()>3){
          LinkedHashMap<Field,Object> fields=new LinkedHashMap<>();
          fields.put(new Field("DLBM"),kcdlbm);
          fields.put(new Field("ZLDWDM"),zldwdm);
          fields.put(new Field("TBDLMJ"),tbmj-tbdlmj);

          Field[] extendFields = extentLayerReaderConfig.getAttributes();
          for (Field f: extendFields) {
            String fname = f.getName();
            fields.put(f, feature.getAttribute(fname));
          }

          Feature f = new Feature(feature);
          f.setAttributes(fields);

          return new Tuple2<>(input._1,f);
        }
        return new Tuple2<>("EMPTY",null);
      }
    }).filterToLayer(new Function<Tuple2<String, Feature>,Boolean>() {
      @Override
      public Boolean call(Tuple2<String, Feature> input) throws Exception {
        return !input._1.equals("EMPTY");
      }
    });

    Layer layer=intersectedLayer.union(kcLayer);

    Layer resultLayer=layer.mapToLayer(new PairFunction<Tuple2<String,Feature>,String,Feature>() {
      @Override
      public Tuple2<String,Feature> call(Tuple2<String,Feature> input) throws Exception {
        Field[] extendFields = extentLayerReaderConfig.getAttributes();
        String[] key = new String[extendFields.length + 2];
        key[0]=input._2.getAttribute("DLBM").toString();
        key[1]=input._2.getAttribute("ZLDWDM").toString();
        for (int i=2; i<key.length; i++) {
          key[i] = input._2.getAttribute(extendFields[i-2]).toString();
        }
        Feature f = new Feature(input._2);
        return new Tuple2<>(StringUtils.join(key, "##"),f);
      }
    });

    Layer resultRDD=resultLayer.reduceToLayer(new Function2<Feature,Feature,Feature>() {
      @Override
      public Feature call(Feature input1, Feature input2) throws Exception {
        Double tbdlmj1=Double.valueOf(input1.getAttribute("TBDLMJ").toString());
        Double tbdlmj2=Double.valueOf(input2.getAttribute("TBDLMJ").toString());
        Double tbdlmj=tbdlmj1+tbdlmj2;
        Feature f=new Feature(input1);
        LinkedHashMap<Field,Object> attr=f.getAttributes();
        for(Field key:attr.keySet()){
          if(key.getName().equals("TBDLMJ")){
            attr.put(key,tbdlmj);
          }
        }
//        f.setAttributes(attr);
        return f;
      }
    }).mapToLayer(new Function<Tuple2<String,Feature>, Tuple2<String,Feature>>() {
      @Override
      public Tuple2<String,Feature> call(Tuple2<String,Feature> input) throws Exception {
        return new Tuple2<String, Feature>(input._2.getFid(), input._2);
      }
    });

//    Map<String, Feature> result = resultRDD.collectAsMap();
//    File file = new File("G:\\DATA\\flow\\out\\result.txt");
//    if (file.exists()) {
//      file.delete();
//    }
//    FileWriter writer = new FileWriter(file);
//    for(String key :result.keySet()){
//      writer.write(key+"##"+result.get(key).getAttribute("TBDLMJ")+"\t\n");
//    }
//    writer.close();
//    StatLayer statLayer = layer.aggregateByField(this.arg.getAggregateFieldName());
//
    LayerWriterConfig writerConfig = LayerFactory.getWriterConfig(this.arg.getStatsWriterConfig());
    LayerWriter resultWriter = LayerFactory.getWriter(this.ss, writerConfig);
    resultWriter.write(resultLayer);
  }


  public static void main(String[] args) throws Exception {
    LandUseAnalysis analysis = new LandUseAnalysis(SparkSessionType.LOCAL, args);
    analysis.exec();
  }

}
