package edu.zju.gis.hls.ml;

import edu.zju.gis.hls.trajectory.doc.ml.NNDitributeTrainModel;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.deeplearning4j.spark.datavec.DataVecDataSetFunction;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import scala.Tuple2;

import java.util.List;
import java.util.UUID;

/**
 * @author Hu
 * @date 2020/12/28
 **/
public class NNDistributeModelTest {

  private static String FILEPATH = "D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\out\\test.csv";

  public static void main(String[] args) {

    SparkSession ss = SparkSession.builder().master("local[4]").appName("GenerateTrainData").getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
    NNDitributeTrainModel model = new NNDitributeTrainModel(ss);
    model.setNumEpochs(100);

    // map data to DataSet
    JavaRDD<String> s = jsc.textFile(FILEPATH);
    RecordReader reader = new NNModelTest.TrajectoryCSVRecordReader();
    JavaRDD<List<Writable>> rddWritables = s.map(new StringToWritablesFunction(reader));

    int labelIndexFrom = 0;
    int labelIndexTo = 0;

    JavaRDD<DataSet> ds = rddWritables.map(new DataVecDataSetFunction(labelIndexFrom, labelIndexTo,
      true, null, null));

    ds.cache();

    List<DataSet> samples = ds.take(10);
    JavaPairRDD<String, DataSet> sampleRDD = jsc.parallelize(samples).mapToPair(x-> new Tuple2<String, DataSet>(UUID.randomUUID().toString(), x));
    sampleRDD.cache();
    JavaPairRDD<String, INDArray> sampleFeaturesRDD = sampleRDD.mapToPair(x-> new Tuple2<String, INDArray>(x._1, x._2.getFeatures()));

    System.out.println("=== MODEL TRAINNING ===");
    model.train(ds);
    System.out.println("=== MODEL TESTING ===");
    model.evaluate(ds);
    System.out.println("=== MODEL INFERING ===");
    System.out.println("=== ORIGINS ===");
    sampleRDD.collect().forEach(x->System.out.println(x._1 + ": " + x._2.getLabels().toStringFull()));
    System.out.println("=== INFERRESULTS ===");
    List<Tuple2<String, INDArray>> result = model.infer(sampleFeaturesRDD, 10).collect();
    result.forEach(x -> System.out.println(x._1 + ": " + x._2.toStringFull()));

    ss.stop();
    ss.close();
  }

}
