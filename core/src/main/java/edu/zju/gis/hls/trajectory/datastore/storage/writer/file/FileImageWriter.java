package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.datastore.storage.writer.ImageWriter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * 图片写出到文件
 * @author Keran Sun (katus)
 * @version 1.0, 2020-08-13
 */
public class FileImageWriter extends ImageWriter {
  private FileImageWriterConfig writerConfig;

  public FileImageWriter(SparkSession ss, FileImageWriterConfig writerConfig) {
    super(ss);
    this.writerConfig = writerConfig;
  }

  @Override
  public void write(JavaPairRDD<String, BufferedImage> imageJavaPairRDD) {
    List<Tuple2<String, BufferedImage>> resultList = imageJavaPairRDD.collect();
    for (Tuple2<String, BufferedImage> result : resultList) {
      String[] keys = result._1().split("_");
      BufferedImage img = result._2();
      File dir = new File(writerConfig.getBaseDir() + "/" + keys[0] + "/" + keys[1]);
      if (!dir.exists()) dir.mkdirs();
      File output = new File(writerConfig.getBaseDir() + "/" + keys[0] + "/" + keys[1] + "/" + keys[2] + ".png");
      try {
        ImageIO.write(img, "png", output);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
