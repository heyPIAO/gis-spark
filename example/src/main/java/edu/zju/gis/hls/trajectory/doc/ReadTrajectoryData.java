package edu.zju.gis.hls.trajectory.doc;

import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.trajectory.analysis.model.*;
import edu.zju.gis.hls.trajectory.analysis.proto.TemporalPoint;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPointLayer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.TrajectoryPolylineLayer;
import edu.zju.gis.hls.trajectory.datastore.storage.LayerFactory;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.LayerReader;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import edu.zju.gis.hls.trajectory.sql.util.SparkSqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


@Slf4j
public class ReadTrajectoryData {

    public static void main(String[] args) throws Exception {

        SparkSession ss = SparkSqlUtil.createSparkSessionWithSqlExtent(SparkSessionType.LOCAL, "ReadTrajectoryData");

        FileLayerReaderConfig layerReaderConfig = new FileLayerReaderConfig();
        layerReaderConfig.setSourcePath("file:///E://2020data/part-00001");
        layerReaderConfig.setLayerType(LayerType.TRAJECTORY_POINT_LAYER);
        layerReaderConfig.setLayerName("CarTripPointLayer");

        Field timeField = new LongField("timestamp");
        timeField.setFieldType(FieldType.TIME_FIELD);
        timeField.setIndex(1);
        layerReaderConfig.setTimeField(timeField);

        Field shapeField = new Field("shape", 2, FieldType.SHAPE_FIELD);
        layerReaderConfig.setShapeField(shapeField);
        shapeField.setType(TemporalPoint.class);

        Field carIdField = new Field("carId", 0, FieldType.NORMAL_FIELD);
        layerReaderConfig.setAttributes(new Field[]{carIdField});

        LayerReader<TrajectoryPointLayer> reader = LayerFactory.getReader(ss, layerReaderConfig);
        TrajectoryPointLayer layer = reader.read();

        TrajectoryPolylineLayer trajectoryLayer = layer.convertToPolylineLayer(carIdField.getName());

        Dataset<Row> row = trajectoryLayer.toGeomDataset(ss);
        row.printSchema();

        row.createOrReplaceTempView("CarTripLayer");

        Dataset<Row> m = ss.sql("select * from CarTripLayer where st_contains(st_makeBBOX(116.184, 116.609, 39.739, 40.103), shape)");
        m.explain(true);
        m.show(false);

        ss.close();
        ss.stop();

    }

}
