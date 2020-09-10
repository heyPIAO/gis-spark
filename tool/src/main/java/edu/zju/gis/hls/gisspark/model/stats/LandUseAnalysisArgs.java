package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.args.BaseArgs;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
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
  private LayerReaderConfig extentReaderConfig; // 范围图层

  @Option(name = "-target",usage = "被统计图层,json", required = true)
  private LayerReaderConfig targetReaderConfig; // 被统计图层

  @Option(name = "-statOutput",usage = "分析结果存储位置,json", required = true)
  private LayerWriterConfig statsWriterConfig;

  @Option(name = "-aggregateFieldName",usage = "聚合字段名称", required = true)
  private String aggregateFieldName;
}
