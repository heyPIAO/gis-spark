package edu.zju.gis.hls.ml;

import edu.zju.gis.hls.trajectory.doc.ml.NNDitributeTrainModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.deeplearning4j.spark.datavec.DataVecDataSetFunction;
import org.elasticsearch.common.collect.Tuple;
import org.nd4j.linalg.api.buffer.DataBuffer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import scala.Array;
import scala.Tuple2;

import java.util.*;

public class LearnIndexDistributeTest {
    private static String FILEPATH = "E:\\2020data\\test.csv";
    private JavaPairRDD<Integer, DataSet> dss;

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().master("local[4]").appName("GenerateTrainData").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
        Vector<NNDitributeTrainModel> modelset = new Vector<NNDitributeTrainModel>();
        Vector<Float> errbounds=new Vector<Float>();

        // map data to DataSet
        JavaRDD<String> s = jsc.textFile(FILEPATH);
        RecordReader reader = new NNModelTest.TrajectoryCSVRecordReader();
        JavaRDD<List<Writable>> rddWritables = s.map(new StringToWritablesFunction(reader));

        int labelIndexFrom = 0;
        int labelIndexTo = 0;

        JavaRDD<DataSet> ds = rddWritables.map(new DataVecDataSetFunction(labelIndexFrom, labelIndexTo,
                true, null, null));
        JavaPairRDD<Integer,DataSet> dss= ds.mapToPair(new PairFunction<DataSet, Integer, DataSet>() {
            @Override
            public Tuple2<Integer, DataSet> call(DataSet dataSet) throws Exception {
                return new Tuple2<Integer,DataSet>((dataSet.getFeatures().hashCode()%10),dataSet);
            }
        });
        for(int i=0;i<10;i++)
        {
            NNDitributeTrainModel model = new NNDitributeTrainModel(ss);
            model.setNumEpochs(100);
            int finalI = i;
            JavaPairRDD<Integer,DataSet> subdsp=dss.filter(new Function<Tuple2<Integer, DataSet>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Integer, DataSet> integerDataSetTuple2) throws Exception {
                    if(integerDataSetTuple2._1== finalI)
                        return true;
                    return false;
                }
            });
            //计算每个子集四个向量的最大最小值
            JavaPairRDD<Integer,DataSet> submax=subdsp.reduceByKey(new Function2<DataSet, DataSet, DataSet>() {
                @Override
                public DataSet call(DataSet dataSet, DataSet dataSet2) throws Exception {
                    INDArray features=dataSet2.getFeatures();
                    DataBuffer dbfeature=features.data();
                    INDArray labels=dataSet2.getLabels();
                    DataBuffer dblabel=labels.data();
                    for(int i=0;i<3;i++)
                    {
                        if(dataSet.getFeatures().data().getFloat(i)>dataSet2.getFeatures().data().getFloat(i))
                        {
                            dbfeature.put(i,dataSet.getFeatures().data().getFloat(i));
                        }
                    }
                    if(dataSet.getLabels().data().getFloat(0)>dataSet2.getLabels().data().getFloat(0))
                    {
                        dblabel.put(0,dataSet.getLabels().data().getFloat(0));
                    }
                    features.setData(dbfeature);
                    labels.setData(dblabel);
                    return new DataSet(features,labels,null,null);
                }
            });
            JavaPairRDD<Integer,DataSet> submin=subdsp.reduceByKey(new Function2<DataSet, DataSet, DataSet>() {
                @Override
                public DataSet call(DataSet dataSet, DataSet dataSet2) throws Exception {
                    INDArray features=dataSet2.getFeatures();
                    DataBuffer dbfeature=features.data();
                    INDArray labels=dataSet2.getLabels();
                    DataBuffer dblabel=labels.data();
                    for(int i=0;i<3;i++)
                    {
                        if(dataSet.getFeatures().data().getFloat(i)<dataSet2.getFeatures().data().getFloat(i))
                        {
                            dbfeature.put(i,dataSet.getFeatures().data().getFloat(i));
                        }
                    }
                    if(dataSet.getLabels().data().getFloat(0)<dataSet2.getLabels().data().getFloat(0))
                    {
                        dblabel.put(0,dataSet.getLabels().data().getFloat(0));
                    }
                    features.setData(dbfeature);
                    labels.setData(dblabel);
                    return new DataSet(features,labels,null,null);
                }
            });
            List<Tuple2<Integer, DataSet>> mint=submin.collect();
            List<Tuple2<Integer, DataSet>> maxt=submax.collect();
            //对子集进行归一化处理
            JavaRDD<DataSet> subds=subdsp.map(new Function<Tuple2<Integer, DataSet>, DataSet>() {
                @Override
                public DataSet call(Tuple2<Integer, DataSet> integerDataSetTuple2) throws Exception {
                    DataSet origin=integerDataSetTuple2._2;
                    DataBuffer dbfeature=origin.getFeatures().data();
                    DataBuffer dblabel=origin.getLabels().data();
                    DataSet min=mint.get(0)._2;
                    DataSet max=maxt.get(0)._2;
                    for(int i=0;i<3;i++)
                    dbfeature.put(i,(origin.getFeatures().data().getFloat(i)-min.getFeatures().data().getFloat(i))/
                                        (max.getFeatures().data().getFloat(i)-min.getFeatures().data().getFloat(i)));
                    dblabel.put(0,(origin.getLabels().data().getFloat(0)-min.getLabels().data().getFloat(0))/
                            (max.getLabels().data().getFloat(0)-min.getLabels().data().getFloat(0)));
                    INDArray features=origin.getFeatures();
                    INDArray labels=origin.getLabels();
                    features.setData(dbfeature);
                    labels.setData(dblabel);
                    origin.setFeatures(features);
                    origin.setLabels(labels);
                    return origin;
                }
            });
            subds.cache();
            model.train(subds);
            model.evaluate(subds);
            List<DataSet> samples = subds.collect();
            JavaPairRDD<String, DataSet> sampleRDD = jsc.parallelize(samples).mapToPair(x-> new Tuple2<String, DataSet>(UUID.randomUUID().toString(), x));
            sampleRDD.cache();
            JavaPairRDD<String, INDArray> sampleFeaturesRDD = sampleRDD.mapToPair(x-> new Tuple2<String, INDArray>(x._1, x._2.getFeatures()));
            List<Tuple2<String, INDArray>> result = model.infer(sampleFeaturesRDD, samples.size()).collect();
            JavaPairRDD<String, INDArray> resultRDD = jsc.parallelize(result).mapToPair((PairFunction<Tuple2<String, INDArray>, String, INDArray>) stringINDArrayTuple2 -> stringINDArrayTuple2);
            JavaPairRDD<String, Tuple2<INDArray, DataSet>> join = resultRDD.join(sampleRDD);
            JavaRDD<Float> floatJavaRDD = join.flatMap(new FlatMapFunction<Tuple2<String, Tuple2<INDArray, DataSet>>, Float>() {
                @Override
                public Iterator<Float> call(Tuple2<String, Tuple2<INDArray, DataSet>> stringTuple2Tuple2) throws Exception {
                    float temp = Math.abs(stringTuple2Tuple2._2._2.getLabels().data().getFloat(0) -
                            stringTuple2Tuple2._2._1.data().getFloat(0));
                    return Arrays.asList(temp).iterator();
                }
            });
            Float reduce = floatJavaRDD.reduce(new Function2<Float, Float, Float>() {
                @Override
                public Float call(Float aFloat, Float aFloat2) throws Exception {
                    return Math.max(aFloat, aFloat2);
                }
            });
            errbounds.add(reduce);
            modelset.add(model);

        }
        ss.stop();
        ss.close();

    }
}
