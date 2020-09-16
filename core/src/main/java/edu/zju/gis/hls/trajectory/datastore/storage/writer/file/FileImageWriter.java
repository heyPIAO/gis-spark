package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.analysis.model.Point;
import edu.zju.gis.hls.trajectory.analysis.model.RasterImage;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.StatLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.ImageWriter;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author Keran Sun (katus)
 * @version 1.0, 2020-09-16
 */
public class FileImageWriter extends ImageWriter {
    private final FileImageWriterConfig writerConfig;

    public FileImageWriter(SparkSession ss, FileImageWriterConfig writerConfig) {
        super(ss);
        this.writerConfig = writerConfig;
    }

    @Override
    public void write(StatLayer imageLayer) {

        List<Tuple2<String, Point>> resultList = imageLayer.collect();
        for (Tuple2<String, Point> result : resultList) {
            String[] keys = result._1().split("_");
            BufferedImage img = ((RasterImage)(result._2().getAttribute("img"))).getImg();
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
