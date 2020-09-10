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
 * 三调地类流转
 **/
@Getter
@Setter
public class LandFlowAnalysis3DArgs extends BaseArgs {

  @Option(name = "-layer1",usage = "图斑三调图层读取参数定义,json", required = true)
  private LayerReaderConfig layer1ReaderConfig; // 三调图层一

  @Option(name = "-layer2",usage = "图斑二调图层读取参数定义,json", required = true)
  private LayerReaderConfig layer2ReaderConfig; // 二调图层二

  @Option(name = "-attrReserved",usage = "是否保留两个图层的字段信息", required = false)
  private Boolean attrReserved = Boolean.TRUE;

  @Option(name = "-writer",usage = "结果图层写出,json", required = true)
  private LayerWriterConfig writerConfig; // 二调图层二

}
