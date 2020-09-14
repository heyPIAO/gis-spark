package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.exception.ModelFailedException;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2020/9/14
 * 平差调整
 * Hint：输入控制总面积，及需要被平差的各个面积Feature及平差字段，调整被平差字段的值，使其与总面积相符
 **/
@Slf4j
public class AreaAdjustment {

  /**
   * 根据控制总面积平差
   * @param tarea 控制总面积
   * @param field 被平差字段
   * @param features 被平差图斑
   * @param scale 精度位数
   * @return
   */
  public static List<Feature> adjust(Double tarea, Field field, List<Feature> features, int scale) {
    return AreaAdjustment.adjust(tarea, field.getName(), features, scale);
  }

  // TODO 待测排序顺序
  public static List<Feature> adjust(Double tarea, String fieldName, List<Feature> features, int scale) {
    long tareal = Math.round(tarea*Math.pow(10, scale));
    if (features.size() == 0) {
      log.warn("Area adjustment feature length is nil, please check");
      log.warn("Area adjustment do nothing, return");
      return features;
    }
    List<Feature> outFeatures = new ArrayList<>();
    // 首先计算指定字段按精度取舍后的值
    long total = 0L;
    for (Feature f: features) {
      Feature of = new Feature(f);
      Object o = of.getAttribute(fieldName);
      if (!(o instanceof Number)) {
        throw new ModelFailedException("Area adjustment field type must be numeric");
      }
      Double d = Double.valueOf(String.valueOf(o));
      long dl = Math.round(d*Math.pow(10, scale));
      of.updateAttribute(fieldName, dl);
      outFeatures.add(of);
      total += dl;
    }

    long diff = tareal - total;
    if (diff < 0) {
      log.warn("Area adjustment differential is negative, abnormal, do nothing");
      return features;
    }

    int length = features.size();
    long plus = diff / length; // 取整，每个图斑需要被平差的量
    long mod = diff % length; // 求余
    // 排序，然后将余数加上，再将取整的数加上
    // 按被平差字段降序
    outFeatures.sort((o1, o2) -> -((int)((long)o1.getAttribute(fieldName)-(long)o2.getAttribute(fieldName))));
    for (int i=0; i<mod; i++) {
      Feature f = outFeatures.get(i);
      long origin = (long)f.getAttribute(fieldName);
      f.updateAttribute(fieldName, origin+1);
    }
    for (int i=0; i<length; i++) {
      Feature f = outFeatures.get(i);
      long origin = (long)f.getAttribute(fieldName);
      f.updateAttribute(fieldName, origin+plus);
    }

    outFeatures.forEach(f-> {
      Object o = f.getAttribute(fieldName);
      Double d = Double.valueOf(String.valueOf(o));
      f.updateAttribute(fieldName, d/Math.pow(10, scale));
    });
    return outFeatures;
  }

}
