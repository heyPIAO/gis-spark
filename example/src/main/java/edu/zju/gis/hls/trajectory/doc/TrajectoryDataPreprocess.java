package edu.zju.gis.hls.trajectory.doc;

import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.gisspark.model.util.SparkUtil;
import edu.zju.gis.hls.trajectory.analysis.util.DateUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 北京出租车点对数据的预处理
 */
public class TrajectoryDataPreprocess {


    public static void main(String[] args) {

        SparkSession ss = SparkUtil.getSparkSession(SparkSessionType.LOCAL, "ReadTrajectoryData");

        JavaRDD<String> lines = ss.sparkContext().textFile("/home/DaLunWen/data/trajectory/test_10",
                ss.sparkContext().defaultParallelism()).toJavaRDD().map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String[] fields = s.split(",");
                SimpleDateFormat format = DateUtils.getGenenalParser();
                Date timestamp = format.parse(fields[1]);
                Double lon = Double.valueOf(fields[2]);
                Double lat = Double.valueOf(fields[3]);
                GeometryFactory gf = new GeometryFactory();
                Point point = gf.createPoint(new Coordinate(lon, lat));
                String[] out = new String[fields.length-1];
                out[0] = fields[0];
                out[1] = String.valueOf(timestamp.getTime());
                out[2] = point.toText();
                return StringUtils.join(out, "\t");
            }
        });

        lines.saveAsTextFile("/home/DaLunWen/data/trajectory/test_10_wkt/");

        ss.close();
        ss.stop();
    }


}
