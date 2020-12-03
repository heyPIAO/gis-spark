package edu.zju.gis.hls.gisspark.model.args;

import edu.zju.gis.hls.trajectory.analysis.model.Term;
import lombok.Getter;
import org.kohsuke.args4j.Option;

/**
 * @author Hu
 * @date 2020/7/14
 **/
@Getter
public class DataLoaderArgs extends BaseArgs {

  @Option(name = "-output",usage = "输出数据图层定义,json", required = true)
  private String output;

  @Option(name = "-input",usage = "输入数据图层定义,json", required = true)
  private String input;

  @Option(name = "-targetCrs",usage = "输出数据目标投影", required = false)
  private String targetCrs = Term.DEFAULT_CRS.toWKT();

}
