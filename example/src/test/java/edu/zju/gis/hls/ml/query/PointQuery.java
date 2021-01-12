package edu.zju.gis.hls.ml.query;

import edu.zju.gis.hls.trajectory.analysis.index.ml.model.NNModel;
import edu.zju.gis.hls.trajectory.analysis.index.ml.model.NNModelMeta;
import edu.zju.gis.hls.trajectory.doc.model.TrainingRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.io.ParseException;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Hu
 * @date 2021/1/9
 * 基于ML的点数据查询
 * 1维训练与查询
 * TODO java 中 array 的长度不能超过int范围，即：2^31-1
 **/
public class PointQuery {

  private static int EPOCH = 2000;
  private static double SCORE_THRES = 2;
  private static String MODEL_SAVE_PATH = "D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\model";
  private static Integer INPUT_DIMENSION = 2;
  private static Integer OUTPUT_DIMENSION = 1;
  private static String queryWkt =
    "POLYGON ((116.0 38.0, 117.0 38.0, 117.0 40.0, 116.0 40.0, 116.0 38.0))";
  private static Long[] TIME_WINDOW = { 1202249308000L, 1202433359000L };

  public static void main(String[] args) throws IOException, ParseException {

    String filePath = "file:///D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\spatialPoint\\single_dim_x\\*";

    // 读取数据，构建序列
    SparkSession ss = SparkSession.builder().master("local[1]").appName("PointQuery").getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(ss.sparkContext());
    JavaRDD<TrainingRecord> recordsRDD = jsc.textFile(filePath).map(TrainingRecord::new);
    List<TrainingRecord> recordsList = recordsRDD.collect();

    ss.stop();
    ss.close();

    TrainingRecord[] records = new TrainingRecord[recordsList.size()];
    records = recordsList.toArray(records);

    recordsList = null;
    // 训练nn模型
//    nnModel(records);
  }


  public static void nnModel(TrainingRecord[] records) throws IOException {
    printInfo(" =========== nnModel ========== ");
    // 设置模型
    NNModel nnModel = new NNModel(INPUT_DIMENSION, OUTPUT_DIMENSION);
    nnModel.setId("spatial_model1");
    // 设置数据
    double[][] inputs = new double[records.length][INPUT_DIMENSION];
    double[][] labels = new double[records.length][OUTPUT_DIMENSION];
    for (int i=0; i<records.length; i++) {
      TrainingRecord record = records[i];
      double[] feature = new double[INPUT_DIMENSION];
      feature[0] = record.getScaledX();
      feature[1] = record.getScaledY();
      if (INPUT_DIMENSION == 3) feature[2] = record.getScaledTime();
      inputs[i] = feature;
      labels[i] = new double[]{ record.getScaledIndex() };
    }

    INDArray Iinputs = Nd4j.create(inputs);
    INDArray Ilabels = Nd4j.create(labels);
    DataSet ds = new DataSet(Iinputs, Ilabels);
    // 模型训练
    nnModel.setNumEpochs(EPOCH);
    nnModel.setScoreThreshold(SCORE_THRES * 1.0/records.length);
    long startTime = System.currentTimeMillis();
    nnModel.train(ds);
    long endTime = System.currentTimeMillis();
    printInfo("TrainingTime: " + (endTime-startTime));

    // 计算 bias
    startTime = System.currentTimeMillis();
    INDArray predicts = nnModel.output(ds.getFeatures()).muli(records.length);
    INDArray bias = predicts.sub(Ilabels.mul(records.length));
    endTime = System.currentTimeMillis();
    printInfo("Bias Generate Time: " + (endTime-startTime));

    int[][] intevals = new int[1][2];
    intevals[0][0] = bias.minNumber().intValue() - 1;
    intevals[0][1] = bias.maxNumber().intValue() + 1;

    NNModelMeta meta = new NNModelMeta();
    meta.setId(nnModel.getId());
    meta.setBias(intevals);

    // 模型保存
    nnModel.save(MODEL_SAVE_PATH + File.separator + nnModel.getId());
    meta.save(MODEL_SAVE_PATH);
    printInfo("Model Saved");
  }

  public static void printInfo(String s) {
    System.out.println(s);
  }

}
