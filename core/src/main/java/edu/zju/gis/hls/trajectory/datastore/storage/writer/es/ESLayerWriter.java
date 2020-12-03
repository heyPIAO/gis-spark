package edu.zju.gis.hls.trajectory.datastore.storage.writer.es;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.json.JSONObject;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static edu.zju.gis.hls.trajectory.analysis.model.FieldType.*;

/**
 * @author Hu
 * @date 2020/7/1
 **/
@Slf4j
public class ESLayerWriter extends LayerWriter<Map<String, Object>> {

    @Getter
    @Setter
    private ESLayerWriterConfig config;

    public ESLayerWriter(SparkSession ss, ESLayerWriterConfig config) {
        super(ss);
        this.config = config;
    }

    @Override
    public Map<String, Object> transform(Feature feature) {
        Map<String, Object> row = new HashMap<>();
        feature.getAttributes().forEach((k, v) -> {
            Field field = (Field) k;
            row.put(field.getName(), v);
        });

        //TODO 原本的空间内字段名称丢失了
        try {
            String geoStr = feature.getGeometryJson();
            row.put(SHAPE_FIELD.getFname(), new JSONObject(geoStr).toMap());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        //添加id
        row.put(ID_FIELD.getFname(), feature.getFid());
        return row;
    }

    @Override
    public void write(Layer layer) {
        //根据字段信息，新建index
        if (!this.createIndex(layer)) {
            return;
        }
        //填充信息
        JavaRDD<Map<String, ?>> rdd = ((JavaRDD<Tuple2<String, Feature>>) (layer.rdd().toJavaRDD())).map(x -> x._2).map(x -> transform(x));
        //使得图层的FID映射到index的_id
        JavaEsSpark.saveToEs(rdd, this.config.getResource(), ImmutableMap.of("es.mapping.id", ID_FIELD.getFname()));
    }

    private boolean createIndex(Layer layer) {

        String ip = this.config.getMasterNode();

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ip, Integer.valueOf(this.config.getPort()))));

        log.info("Create index: " + this.config.getIndexName());

        // index exist
        GetIndexRequest indexExistRequst = new GetIndexRequest(this.config.getIndexName());
        try {
            boolean exists = client.indices().exists(indexExistRequst, RequestOptions.DEFAULT);
            if (exists) {
                log.error(String.format("Index already exists: %s", this.config.getIndexName()));
                return false;
            }
        } catch (Exception e) {
            log.error("Index existed test failed.");
            return false;
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(this.config.getIndexName());
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", this.config.getNUMBER_OF_SHARDS())
                .put("index.number_of_replicas", this.config.getNUMBER_OF_REPLICA())
                .put("index.refresh_interval", this.config.getINDEX_REFRESH_INTERVAL())
        );
        Map<Field, Object> fields = layer.getMetadata().getAttributes();
        Map<String, Object> properties = new HashMap<>();
        fields.forEach((k, v) -> {
            //TODO 找到定义_id的地方
            if (!k.getName().equals("_id")) {
                Map<String, String> fieldMap = new HashMap<>();
                if (k.getType().equals(Integer.class.getName())) {
                    fieldMap.put("type", "integer");
                } else if (k.getType().equals(Double.class.getName())) {
                    fieldMap.put("type", "double");
                } else if (k.getType().equals(String.class.getName())) {
                    fieldMap.put("type", "text");
                } else if (k.getType().equals(Geometry.class.getName())) {
                    fieldMap.put("type", "geo_shape");
                } else {
                    fieldMap.put("type", "text");
                }
                properties.put(k.getName(), fieldMap);
            }
        });
        Map<String, String> fidMap = new HashMap<>();
        fidMap.put("type", "text");
        properties.put(ID_FIELD.getFname(), fidMap);
        Map<String, String> shaepMap = new HashMap<>();
        shaepMap.put("type", "geo_shape");
        properties.put(SHAPE_FIELD.getFname(), shaepMap);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        Gson gson = new Gson();
        createIndexRequest.mapping(gson.toJson(mapping), XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            if (createIndexResponse.isAcknowledged()) {
                log.info(String.format("create index %s success", this.config.getIndexName()));
            } else {
                log.error(String.format("create index %s fail", this.config.getIndexName()));
            }
            client.close();
            return true;
        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        }
    }
}
