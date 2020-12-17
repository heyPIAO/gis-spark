package edu.zju.gis.hls.trajectory.doc;

/**
 * @author Hu
 * @date 2020/12/8
 * 二维点数据基于 hilbert curve 和 x 精度的训练数据生成
 **/
public class H2PointTrainData {

  private static String TDRIVE_DIR = "D:\\Work\\DaLunWen\\data\\T-drive Taxi Trajectories\\release\\taxi_log_2008_by_id";
  private static String FILE_TYPE = ".txt";

  public static void main(String[] args) {
    generateSmall();
  }

  // 用10条T-drive出租车数据生成
  private static void generateSmall() {
    String[] filenames = new String[10];
    for (int i=1; i<=10; i++) {
      filenames[i-1] = String.valueOf(i);
    }
  }

  private static void generate(String[] filenames) {
    
  }

}
