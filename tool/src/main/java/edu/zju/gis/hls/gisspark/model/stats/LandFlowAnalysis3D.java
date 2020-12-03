package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.BaseModel;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.index.DistributeSpatialIndex;
import edu.zju.gis.hls.trajectory.analysis.index.IndexType;
import edu.zju.gis.hls.trajectory.analysis.index.SpatialIndexFactory;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.UniformGridIndexConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.MultiPolygon;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.MultiPolygonLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * @author Hu
 * @date 2020/9/10
 **/
public class LandFlowAnalysis3D extends BaseModel<LandFlowAnalysis3DArgs> {

  private static Integer DEFAULT_INDEX_LEVEL = 14;

  public LandFlowAnalysis3D(SparkSessionType type, String[] args) {
    super(type, args);
  }

  @Override
  protected void run() throws Exception {

    LayerReaderConfig layer1ReaderConfig=LayerFactory.getReaderConfig(this.arg.getLayer1ReaderConfig());
    LayerReaderConfig layer2ReaderConfig=LayerFactory.getReaderConfig(this.arg.getLayer2ReaderConfig());
    LayerReader layer1Reader = LayerFactory.getReader(this.ss, layer1ReaderConfig);
    LayerReader layer2Reader = LayerFactory.getReader(this.ss, layer2ReaderConfig);

    MultiPolygonLayer layer1 = (MultiPolygonLayer)layer1Reader.read();
    MultiPolygonLayer layer2 = (MultiPolygonLayer)layer2Reader.read();

    Layer gareaLayer1=layer1.mapToLayer(new PairFunction<Tuple2<String, MultiPolygon>, String, MultiPolygon>() {
      @Override
      public Tuple2<String, MultiPolygon> call(Tuple2<String, MultiPolygon> input) throws Exception {
        MultiPolygon multiPolygon=input._2;
        LinkedHashMap<Field,Object> attr=multiPolygon.getAttributes();
        Double yarea=multiPolygon.getGeometry().getArea();
        Field field=new Field("YTBMJ");
        attr.put(field,yarea);
        multiPolygon.setAttributes(attr);
        return new Tuple2<>(input._1,multiPolygon);
      }
    });

    Layer gareaLayer2=layer2.mapToLayer(new PairFunction<Tuple2<String, MultiPolygon>, String, MultiPolygon>() {
      @Override
      public Tuple2<String, MultiPolygon> call(Tuple2<String, MultiPolygon> input) throws Exception {
        MultiPolygon multiPolygon=input._2;
        LinkedHashMap<Field,Object> attr=multiPolygon.getAttributes();
        Double yarea=multiPolygon.getGeometry().getArea();
        Field field=new Field("YTBMJ");
        attr.put(field,yarea);
        multiPolygon.setAttributes(attr);
        return new Tuple2<>(input._1,multiPolygon);
      }
    });

    DistributeSpatialIndex si = SpatialIndexFactory.getDistributedSpatialIndex(IndexType.UNIFORM_GRID, new UniformGridIndexConfig(DEFAULT_INDEX_LEVEL, false));

    KeyIndexedLayer indexedLayer1 = si.index(gareaLayer1);
    KeyIndexedLayer indexedLayer2 = si.index(gareaLayer2);

    KeyIndexedLayer<MultiPolygonLayer> resultLayer = indexedLayer1.intersect(indexedLayer2, this.arg.getAttrReserved());
    //TODO 去重
    resultLayer.makeSureCached();
    ArrayList<String>  dlbm_GD_list=new ArrayList<>();
    dlbm_GD_list.add("0101");

    JavaPairRDD<String,ArrayList<HashMap<String,Object>>> testRDD=resultLayer.getLayer().mapToPair(new PairFunction<Tuple2<String, MultiPolygon>, String, ArrayList<HashMap<String,Object>>>() {
      @Override
      public Tuple2<String, ArrayList<HashMap<String,Object>>> call(Tuple2<String, MultiPolygon> input) throws Exception {
        MultiPolygon multiPolygon=input._2;

        String qdlbm=multiPolygon.getAttribute("DLBM_1").toString();
        String qkcdlbm=multiPolygon.getAttribute("KCDLBM_1").toString();
        //String qzldwdm=multiPolygon.getAttribute("ZLDWDM_1").toString();
        Double qtbmj=Double.valueOf(multiPolygon.getAttribute("TBMJ_1").toString());
        Double qtbdlmj=Double.valueOf(multiPolygon.getAttribute("TBDLMJ_1").toString());
        Double qytbmj=Double.valueOf(multiPolygon.getAttribute("YTBMJ_1").toString());

        String hdlbm=multiPolygon.getAttribute("DLBM_2").toString();
        String hkcdlbm=multiPolygon.getAttribute("KCDLBM_2").toString();
        String hzldwdm=multiPolygon.getAttribute("ZLDWDM_2").toString();
        Double htbmj=Double.valueOf(multiPolygon.getAttribute("TBMJ_2").toString());
        Double htbdlmj=Double.valueOf(multiPolygon.getAttribute("TBDLMJ_2").toString());
        Double hytbmj=Double.valueOf(multiPolygon.getAttribute("YTBMJ_2").toString());

        Double xarea=multiPolygon.getGeometry().getArea();
        Double qxtbmj=qtbmj*xarea/qytbmj;//
        Double hxtbmj=htbmj*xarea/hytbmj;//
        Double qxtbdlmj=qtbdlmj*xarea/qytbmj;//
        Double hxtbdlmj=htbdlmj*xarea/hytbmj;//
        //TODO 判断前后地快类编码是否一致
        ArrayList<HashMap<String,Object>> arrayList=new ArrayList<>();
        if(dlbm_GD_list.contains(qdlbm)){
          if(dlbm_GD_list.contains(hdlbm)){
            if(qdlbm.equals(hdlbm)){
                if(qxtbmj-qxtbdlmj-hxtbmj+hxtbdlmj>=0){
                  HashMap<String,Object> map1=new HashMap<>();
                  map1.put("QDLBM",qkcdlbm);
                  map1.put("HDLBM",hdlbm);
                  map1.put("HZLDWDM",hzldwdm);
                  map1.put("MJ",qxtbmj-qxtbdlmj-hxtbmj+hxtbdlmj);
                  arrayList.add(map1);
                  return new Tuple2<>(input._1,arrayList);
                }else if(qxtbmj-qxtbdlmj-hxtbmj+hxtbdlmj<0){
                  HashMap<String,Object> map1=new HashMap<>();
                  map1.put("QDLBM",qdlbm);
                  map1.put("HDLBM",hkcdlbm);
                  map1.put("HZLDWDM",hzldwdm);
                  map1.put("MJ",qxtbmj-qxtbdlmj-hxtbmj+hxtbdlmj);
                  arrayList.add(map1);
                  return new Tuple2<>(input._1,arrayList);
                }
            }else if(!qdlbm.equals(hdlbm)){
                if(qxtbmj-qxtbdlmj>=hxtbmj-hxtbdlmj){
                  HashMap<String,Object> map1=new HashMap<>();
                  map1.put("QDLBM",qdlbm);
                  map1.put("HDLBM",hdlbm);
                  map1.put("HZLDWDM",hzldwdm);
                  map1.put("MJ",qxtbdlmj);
                  arrayList.add(map1);
                  HashMap<String,Object> map2=new HashMap<>();
                  map2.put("QDLBM",qkcdlbm);
                  map2.put("HDLBM",hdlbm);
                  map2.put("HZLDWDM",hzldwdm);
                  map2.put("MJ",qxtbmj-qxtbdlmj-hxtbmj+hxtbdlmj);
                  arrayList.add(map2);
                  return new Tuple2<>(input._1,arrayList);
                }else if(qxtbmj-qxtbdlmj<hxtbmj-hxtbdlmj){
                  HashMap<String,Object> map1=new HashMap<>();
                  map1.put("QDLBM",qdlbm);
                  map1.put("HDLBM",hdlbm);
                  map1.put("HZLDWDM",hzldwdm);
                  map1.put("MJ",hxtbdlmj);
                  arrayList.add(map1);
                  HashMap<String,Object> map2=new HashMap<>();
                  map2.put("QDLBM",qdlbm);
                  map2.put("HDLBM",hkcdlbm);
                  map2.put("HZLDWDM",hzldwdm);
                  map2.put("MJ",hxtbmj-hxtbdlmj-qxtbmj+qxtbdlmj);
                  arrayList.add(map2);
                  return new Tuple2<>(input._1,arrayList);
                }
            }

          }else if(!dlbm_GD_list.contains(hdlbm)){
            HashMap<String,Object> map1=new HashMap<>();
            map1.put("QDLBM",qdlbm);
            map1.put("HDLBM",hdlbm);
            map1.put("HZLDWDM",hzldwdm);
            map1.put("MJ",qxtbdlmj);
            arrayList.add(map1);
            HashMap<String,Object> map2=new HashMap<>();
            map2.put("QDLBM",qkcdlbm);
            map2.put("HDLBM",hdlbm);
            map2.put("HZLDWDM",hzldwdm);
            map2.put("MJ",qxtbmj-qxtbdlmj);
            arrayList.add(map2);
            return new Tuple2<>(input._1,arrayList);
          }
        }else if(!dlbm_GD_list.contains(qdlbm)){
          if(dlbm_GD_list.contains(hdlbm)){
            HashMap<String,Object> map1=new HashMap<>();
            map1.put("QDLBM",qdlbm);
            map1.put("HDLBM",hdlbm);
            map1.put("HZLDWDM",hzldwdm);
            map1.put("MJ",hxtbdlmj);
            arrayList.add(map1);
            HashMap<String,Object> map2=new HashMap<>();
            map2.put("QDLBM",qdlbm);
            map2.put("HDLBM",hkcdlbm);
            map2.put("HZLDWDM",hzldwdm);
            map2.put("MJ",hxtbmj-hxtbdlmj);
            arrayList.add(map2);
            return new Tuple2<>(input._1,arrayList);
          }else if(!dlbm_GD_list.contains(hdlbm)){
            HashMap<String,Object> map1=new HashMap<>();
            map1.put("QDLBM",qdlbm);
            map1.put("HDLBM",hdlbm);
            map1.put("HZLDWDM",hzldwdm);
            map1.put("MJ",hxtbdlmj);
            arrayList.add(map1);
            return new Tuple2<>(input._1,arrayList);
          }
        }

        return null;
      }
    });
    JavaPairRDD<String,HashMap<String,Object>> flatRDD=testRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, ArrayList<HashMap<String, Object>>>, String, HashMap<String, Object>>() {
      @Override
      public Iterator<Tuple2<String, HashMap<String, Object>>> call(Tuple2<String, ArrayList<HashMap<String, Object>>> input) throws Exception {
        ArrayList<Tuple2<String, HashMap<String,Object>>> arrayList = new ArrayList<>();
        for (int i = 0; i < input._2.size(); i++) {
          String qdlbm=input._2.get(i).get("QDLBM").toString();
          String hdlbm=input._2.get(i).get("HDLBM").toString();
          String hzldwdm=input._2.get(i).get("HZLDWDM").toString();
          String id=qdlbm+"##"+hdlbm+"##"+hzldwdm;
          arrayList.add(new Tuple2<String, HashMap<String,Object>>(id, input._2.get(i)));
        }
        return arrayList.iterator();
      }
    });

    JavaPairRDD<String,HashMap<String,Object>> resultRDD=flatRDD.reduceByKey(new Function2<HashMap<String, Object>, HashMap<String, Object>, HashMap<String, Object>>() {
      @Override
      public HashMap<String, Object> call(HashMap<String, Object> input1, HashMap<String, Object> input2) throws Exception {
        Double area1=Double.valueOf(input1.get("MJ").toString());
        Double area2=Double.valueOf(input2.get("MJ").toString());
        input1.put("MJ",area1+area2);
        return input1;
      }
    });

    Map<String,HashMap<String,Object>> result=resultRDD.collectAsMap();
    File file = new File("G:\\DATA\\flow\\out\\result3d.txt");
    if (file.exists()) {
      file.delete();
    }
    FileWriter writer = new FileWriter(file);
    for(String key :result.keySet()){
      writer.write(key+"##"+result.get(key));
    }
    writer.close();

//    LayerWriter geomWriter = LayerFactory.getWriter(this.ss, this.arg.getGeomWriterConfig());
//    geomWriter.write(resultLayer);
//
//    // TODO 流量变化判断如何做？
//    MultiPolygonLayer layer = resultLayer.getLayer();
//    StatLayer statLayer = layer.aggregateByField(this.arg.getAggregateFieldName());
//    LayerWriter statWriter = LayerFactory.getWriter(this.ss, this.arg.getStatWriterConfig());
//    statWriter.write(statLayer);
  }

  public static void main(String[] args) throws Exception {
    LandFlowAnalysis3D analysis3D = new LandFlowAnalysis3D(SparkSessionType.LOCAL, args);
    analysis3D.exec();
  }
}
