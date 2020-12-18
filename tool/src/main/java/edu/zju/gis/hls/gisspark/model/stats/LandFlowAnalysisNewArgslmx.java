package edu.zju.gis.hls.gisspark.model.stats;

import edu.zju.gis.hls.gisspark.model.args.BaseArgs;
import lombok.Getter;
import lombok.Setter;
import org.kohsuke.args4j.Option;

@Getter
@Setter
public class LandFlowAnalysisNewArgslmx extends BaseArgs {

    @Option(name = "-taskName",usage = "任务名称", required = true)
    private String taskName; // 任务名称

    @Option(name = "-year",usage = "数据年份", required = true)
    private String year;

    @Option(name = "-xzqdm",usage = "行政区代码", required = true)
    private String xzqdm;

    @Option(name = "-xzqmc",usage = "行政区名称", required = true)
    private String xzqmc;

    @Option(name = "-tb3d",usage = "图斑三调图层读取参数定义,json", required = true)
    private String tb3dReaderConfig; // 三调图层

    @Option(name = "-tb2d",usage = "图斑二调面状图层读取参数定义,json", required = true)
    private String tb2dReaderConfig; // 二调图层

    @Option(name = "-xzq3d",usage = "行政区三调图层读取参数定义,json", required = true)
    private String xzq3dReaderConfig; // 三调图层

    @Option(name = "-xzq2d",usage = "行政区二调面状图层读取参数定义,json", required = true)
    private String xzq2dReaderConfig; // 二调图层

    @Option(name = "-xz2d",usage = "图斑二调线状图层读取参数定义,json", required = true)
    private String xz2dReaderConfig; // 二调图层

    @Option(name = "-lx2d",usage = "图斑二调点状图层读取参数定义,json", required = true)
    private String lx2dReaderConfig; // 二调图层

    @Option(name = "-geomOutput",usage = "二三调流转空间结果输出参数定义,json", required = true)
    private String geomWriterConfig; // 输出参数

    @Option(name = "-statOutput",usage = "二三调流转统计结果输出参数定义,json", required = true)
    private String statsWriterConfig;

}
