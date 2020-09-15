package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.args.BaseArgs;
import lombok.Getter;
import lombok.Setter;
import org.kohsuke.args4j.Option;

/**
 * @author Hu
 * @date 2020/9/15
 **/
@Getter
@Setter
public class LandFlowPreProcessArgs extends BaseArgs {

  @Option(name = "-lxdw",usage = "零星地物图层读取参数定义,json", required = true)
  private String lxdwReaderConfig; // lxdw

  @Option(name = "-xzdw",usage = "线状地物图层读取参数定义,json", required = true)
  private String xzdwReaderConfig; // xzdw

  @Option(name = "-dltb",usage = "地类图斑图层读取参数定义,json", required = true)
  private String dltbReaderConfig; // dltb

  @Option(name = "-writer",usage = "结果图层输出参数定义,json", required = true)
  private String writerConfig; // 空间数据写出配置

}
