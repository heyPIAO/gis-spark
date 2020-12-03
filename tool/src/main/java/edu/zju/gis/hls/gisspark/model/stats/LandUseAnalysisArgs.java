package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.args.BaseArgs;
import lombok.Getter;
import lombok.Setter;
import org.kohsuke.args4j.Option;

/**
 * @author Hu
 * @date 2020/9/10
 * 土地利用现状分析模型参数
 **/
@Getter
@Setter
public class LandUseAnalysisArgs extends BaseArgs {
  @Option(name = "-extent",usage = "范围图层,json", required = true)
  private String extentReaderConfig; // 范围图层

  @Option(name = "-target",usage = "被统计图层,json", required = true)
  private String targetReaderConfig; // 被统计图层

  @Option(name = "-statOutput",usage = "分析结果存储位置,json", required = true)
  private String statsWriterConfig;

  @Option(name = "-geomOutput",usage = "Intersect结果存储位置,json", required = true)
  private String geomWriterConfig;

}
