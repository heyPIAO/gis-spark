package edu.zju.gis.hls.ml;

import edu.zju.gis.hls.trajectory.analysis.index.ml.model.NNModel;
import lombok.Getter;
import lombok.Setter;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.records.reader.impl.csv.SerializableCSVParser;
import org.datavec.api.split.FileSplit;
import org.datavec.api.writable.Text;
import org.datavec.api.writable.Writable;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2020/12/21
 **/
public class NNModelTest {

  private static Long MIN_INDEX = 0L;
  private static Long MAX_INDEX = 12613L;
  private static Integer BATCH_SIZE = 1000;
  private static String MODEL_SAVE_DIR = "";

  private static int inDimension = 2; // 模型输入维度
  private static int outDimension = 1; // 模型输出维度，对应测试文件的序号
  private static String[] trainData = new String[]{"", "", ""};

  public static void main(String[] args) throws IOException, InterruptedException {
    NNModel model = new NNModel();
    model.setNumEpochs(4000);
    DataSet ds = readCSVDataset("D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\out\\test.csv");
//    DataSet ds = readCSVDataset("D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\out2");
    model.train(ds);

    int nSamples = 10;
    DataSet samples = ds.sample(nSamples);
    INDArray x = samples.getFeatures();
    INDArray labels = samples.getLabels().muli(MAX_INDEX).addi(MIN_INDEX);
    INDArray y = model.output(x).muli(MAX_INDEX).addi(MIN_INDEX);

    System.out.println("== SAMPLES ==");
    System.out.println(x.toStringFull());
    System.out.println("== ORIGINS ==");
    System.out.println(labels.toStringFull());
    System.out.println("== PREDICTS ==");
    System.out.println(y.toStringFull());

    model.save(MODEL_SAVE_DIR + "model_1");
  }

  private static DataSet readCSVDataset(String filename)
    throws IOException, InterruptedException {
    int batchSize = BATCH_SIZE;
    RecordReader rr = new TrajectoryCSVRecordReader();
    rr.initialize(new FileSplit(new File(filename)));

    DataSetIterator iter = new RecordReaderDataSetIterator.Builder(rr, batchSize)
      .regression(0, 0)
      .build();
    return iter.next();
  }

  public static class TrajectoryCSVRecordReader extends CSVRecordReader {

    private SerializableCSVParser csvParser = new SerializableCSVParser();

    public TrajectoryCSVRecordReader() {}

    @Override
    protected List<Writable> parseLine(String line) {
      String[] split;
      try {
        //存疑，为什么要这样读取
        split = this.csvParser.parseLine(line);
        String pointX = split[2];
        String pointY = split[3];
        String time = split[4];
        split = new String[] {split[1], pointX, pointY, time};
      } catch (IOException var8) {
        throw new RuntimeException(var8);
      }

      List<Writable> ret = new ArrayList();
      String[] var4 = split;
      int var5 = split.length;

      for(int var6 = 0; var6 < var5; ++var6) {
        String s = var4[var6];
        ret.add(new Text(s));
      }
      return ret;
    }
  }

}
