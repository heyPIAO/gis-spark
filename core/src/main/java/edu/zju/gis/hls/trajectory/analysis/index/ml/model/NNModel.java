package edu.zju.gis.hls.trajectory.analysis.index.ml.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.BackpropType;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

/**
 * @author Hu
 * @date 2020/12/21
 **/
@Slf4j
public class NNModel implements Serializable {

  @Getter
  @Setter
  private String id = UUID.randomUUID().toString();

  @Getter
  private MultiLayerConfiguration conf;

  @Getter
  transient private MultiLayerNetwork mln;

  @Getter
  @Setter
  private Integer numEpochs = 100; // 默认训练 epoch 数

  @Getter
  @Setter
  private double scoreThreshold = 0.001; // 默认得分变化的比例，少于 0.1%的话，就停止训练。

  public NNModel() {
    this(3,1);
  }


  public NNModel(int inDimension, int outDimension) {
    conf = new NeuralNetConfiguration.Builder()
//      .seed(12345)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .updater(new Nesterovs(0.001, 0.9))
      .l2(1e-3) // weight decay
      .list()
      .layer(0, new DenseLayer.Builder().nIn(inDimension).nOut(100).activation(Activation.RELU).weightInit(WeightInit.XAVIER).build())
      .layer(1, new DenseLayer.Builder().nIn(100).nOut(100).activation(Activation.RELU).weightInit(WeightInit.XAVIER).build())
      .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.L1).nIn(100).nOut(outDimension).activation(Activation.IDENTITY).weightInit(WeightInit.XAVIER).build())
      .backpropType(BackpropType.Standard)
      .build();

    this.mln = new MultiLayerNetwork(conf);
    this.mln.init();
    this.mln.setListeners(new ScoreIterationListener(10));  // 每隔1个iteration就输出一次score
  }

  public void train(DataSet ds) {
    double scoreLast = Double.MIN_VALUE;
    for (int i = 0; i < numEpochs; i++) {
      this.mln.fit(ds);
      double score = this.mln.score();
      if (scoreLast == Double.MIN_VALUE) scoreLast = score;
      else if (Math.abs((score - scoreLast)/scoreLast) < scoreThreshold){
        log.warn("训练score达到设置阈值，结束训练");
        log.info("总的训练 Epoch 为: " + i);
        break;
      }
    }
  }

  public INDArray output(INDArray ds) {
    return this.mln.output(ds);
  }

  public void save(String filename) throws IOException {
    this.mln.save(new File(filename));
  }

  public void load(String filename) throws IOException {
    this.mln = MultiLayerNetwork.load(new File(filename), true);
  }

}
