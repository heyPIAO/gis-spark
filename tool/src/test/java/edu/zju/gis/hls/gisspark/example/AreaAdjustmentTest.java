package edu.zju.gis.hls.gisspark.example;

//import edu.zju.gis.hls.gisspark.model.stats.AreaAdjustment;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.model.FieldType;
import lombok.extern.slf4j.Slf4j;
import org.geotools.geometry.jts.GeometryClipper;
import org.geotools.geometry.jts.LiteCoordinateSequenceFactory;
import org.geotools.geometry.jts.WKTReader2;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.precision.EnhancedPrecisionOp;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2020/9/14
 * 平差工具测试
 **/
@Slf4j
public class AreaAdjustmentTest {

  private static int FEATURE_COUNT = 10;
  private static int SCALE = 2;
  private static Field FIELD = new Field("TBMJ", "TBMJ", Double.class.getName(), 99, 0, FieldType.NORMAL_FIELD);

  public static void main(String[] args) {

    Double realTotalArea = 0.00;
    List<Feature> fs = new ArrayList<>();
    for (int i=0; i<FEATURE_COUNT; i++) {
      Feature f = Feature.empty();
      Double area = 100 * Math.random();
      f.addAttribute(FIELD, area);
      fs.add(f);
      realTotalArea += area;
    }

    Double targetArea = realTotalArea + 0.12;
    log.info(String.format("Total Area before adjust: %.2f", realTotalArea));
    log.info(String.format("Target Area: %.2f", targetArea));
//    List<Feature> result = AreaAdjustment.adjust(targetArea, FIELD.getName(), fs, SCALE);
//    result.forEach(x->log.info(x.toString()));
//    final Double[] total = {0.0};
//    result.forEach(x-> total[0] +=Double.valueOf(String.valueOf(x.getAttribute(FIELD.getName()))));
//    log.info(String.format("Total Area after adjust: %.2f", total[0]));
  }

}
