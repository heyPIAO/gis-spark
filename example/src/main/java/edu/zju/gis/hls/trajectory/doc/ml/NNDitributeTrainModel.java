package edu.zju.gis.hls.trajectory.doc.ml;

import edu.zju.gis.hls.trajectory.analysis.index.ml.model.NNModel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.deeplearning4j.optimize.solvers.accumulation.encoding.threshold.AdaptiveThresholdAlgorithm;
import org.deeplearning4j.spark.api.TrainingMaster;
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer;
import org.deeplearning4j.spark.parameterserver.training.SharedTrainingMaster;
import org.nd4j.evaluation.regression.RegressionEvaluation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.parameterserver.distributed.conf.VoidConfiguration;
import org.nd4j.parameterserver.distributed.v2.enums.MeshBuildMode;

/**
 * @author Hu
 * @date 2020/12/21
 **/
@Slf4j
public class NNDitributeTrainModel {

  private Integer BATCH_SIZE_PER_WORKER = 1000;
  private Integer WORKER_PER_NODE = 1;

  private VoidConfiguration conf;
  private TrainingMaster trainingMaster;
  private SparkDl4jMultiLayer sparkNet;
  // 单节点上的模型设置。一个 MultiLayerConfiguration 或一个 ComputationGraphConfiguration
  private NNModel model;
  private SparkSession ss;

  @Getter
  @Setter
  private Integer numEpochs = 10;

  public NNDitributeTrainModel(SparkSession ss) {
    // 配置梯度共享实现所需的分布式训练
    this(ss, VoidConfiguration.builder()
      .unicastPort(40123) // 工作机将使用于通信的端口。使用任意空闲的端口
//      .networkMask("0.0.0.0/32")     // 用于通信的网络掩码。示例10.0.0.0/24，或192.168.0.0/16等
      .controllerAddress("127.0.0.1")  // 主/驱动器IP
      .meshBuildMode(MeshBuildMode.MESH)
      .build());
  }

  public NNDitributeTrainModel(SparkSession ss, VoidConfiguration conf) {
    this.conf = conf;
    this.model = new NNModel();
    // 参数共享方式实现，也可选择 ParameterAveragingTrainingMaster 参数平均方式
    this.trainingMaster = new SharedTrainingMaster.Builder(this.conf, BATCH_SIZE_PER_WORKER)
      .batchSizePerWorker(BATCH_SIZE_PER_WORKER) // 训练批量大小
      .workersPerNode(WORKER_PER_NODE)      // numWorkersPerNode等于GPU的数量。对于CPU：numWorkersPerNode为1；CPU大核心numWorkersPerNode大于1
      .thresholdAlgorithm(new AdaptiveThresholdAlgorithm(1E-3))
      .build();
    this.ss = ss;
    sparkNet = new SparkDl4jMultiLayer(this.ss.sparkContext(), model.getConf(), trainingMaster);
  }

  public void train(JavaRDD<DataSet> ds) {
    for (int i = 0; i < numEpochs; i++) {
      sparkNet.fit(ds);
    }
  }

  public void evaluate(JavaRDD<DataSet> ds) {
    RegressionEvaluation result = this.sparkNet.evaluateRegression(ds);
    System.out.println(result.stats());
  }

  public JavaPairRDD<String, INDArray> infer(JavaPairRDD<String, INDArray> data, int batchSize) {
    return this.sparkNet.feedForwardWithKey(data, batchSize);
  }

}
