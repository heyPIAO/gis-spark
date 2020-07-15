package edu.zju.gis.hls.gisspark.model.args;

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


}
