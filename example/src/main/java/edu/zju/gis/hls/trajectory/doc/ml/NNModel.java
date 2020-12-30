package edu.zju.gis.hls.trajectory.doc.ml;

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

/**
 * @author Hu
 * @date 2020/12/21
 **/
@Slf4j
public class NNModel {

  @Getter
  private MultiLayerConfiguration conf;

  @Getter
  private MultiLayerNetwork mln;

  @Getter
  @Setter
  private Integer numEpochs = 10;

  public NNModel() {

    conf = new NeuralNetConfiguration.Builder()
      .seed(12345)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .updater(new Nesterovs(0.0001, 0.9))
      .l2(1e-4) // weight decay
      .list()
      .layer(0, new DenseLayer.Builder().nIn(3).nOut(100).activation(Activation.RELU).weightInit(WeightInit.XAVIER).build())
      .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.L1).nIn(100).nOut(1).activation(Activation.IDENTITY).weightInit(WeightInit.XAVIER).build())
      .backpropType(BackpropType.Standard)
      .build();

    this.mln = new MultiLayerNetwork(conf);
    this.mln.init();
    this.mln.setListeners(new ScoreIterationListener(10));  // 每隔1个iteration就输出一次score
  }

  public void train(DataSet ds) {
    for (int i = 0; i < numEpochs; i++) {
      this.mln.fit(ds);
    }
  }

  public INDArray output(INDArray ds) {
    return this.mln.output(ds);
  }

}
