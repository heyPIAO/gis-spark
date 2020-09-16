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
 * 二三调流转分析模型参数
 **/
@Getter
@Setter
public class LandFlowAnalysisArgs extends BaseArgs {

  @Option(name = "-tb3d",usage = "图斑三调图层读取参数定义,json", required = true)
  private String tb3dReaderConfig; // 三调图层

  @Option(name = "-tb2d",usage = "图斑二调图层读取参数定义,json", required = true)
  private String xz2dReaderConfig; // 二调图层

  @Option(name = "-geomOutput",usage = "二三调流转空间结果输出参数定义,json", required = true)
  private String geomWriterConfig; // 输出参数

  @Option(name = "-statOutput",usage = "二三调流转统计结果输出参数定义,json", required = true)
  private String statsWriterConfig;

}
