package edu.zju.gis.hls.ml;

import edu.zju.gis.hls.trajectory.doc.ml.NNDitributeTrainModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.writable.Writable;
import org.datavec.spark.transform.misc.StringToWritablesFunction;
import org.deeplearning4j.spark.datavec.DataVecDataSetFunction;
import org.nd4j.linalg.dataset.DataSet;

import java.util.List;

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

    // map data to DataSet
    JavaRDD<String> s = jsc.textFile(FILEPATH);
    RecordReader reader = new NNModelTest.TrajectoryCSVRecordReader();
    JavaRDD<List<Writable>> rddWritables = s.map(new StringToWritablesFunction(reader));

    int labelIndexFrom = 0;
    int labelIndexTo = 0;

    JavaRDD<DataSet> ds = rddWritables.map(new DataVecDataSetFunction(labelIndexFrom, labelIndexTo,
      true, null, null));

    model.train(ds);

    model.evaluate(ds);

  }

}
