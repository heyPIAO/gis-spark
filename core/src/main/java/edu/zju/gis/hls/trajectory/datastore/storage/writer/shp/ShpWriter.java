package edu.zju.gis.hls.trajectory.datastore.storage.writer.shp;


import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.elasticsearch.index.engine.Engine;
import org.geotools.data.FeatureWriter;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.simple.SimpleFeatureType;
import org.apache.spark.sql.Row;

import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.FeatureCollections;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;


import java.io.File;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import scala.Tuple2;


public class ShpWriter<T extends Layer> extends LayerWriter<Row> {
    @Getter
    @Setter
    private ShpWriterConfig config;

    public ShpWriter(SparkSession ss, ShpWriterConfig config) {
        super(ss);
        this.config = config;
    }

    @Override
    public Row transform(Feature feature) {
        Row row = new GenericRow(feature.toObjectArray());
        return row;
    }

    @Override
    public void write(Layer layer) {
        try{
            List<Tuple2<String,Feature>> re = layer.collect();
            Feature feature_attr = re.get(0)._2;
            LinkedHashMap<Field,Object> attrs = feature_attr.getAttributes();

            File file = new File(this.config.getSinkPath().substring(6));
            Map<String, Serializable> params = new HashMap<String, Serializable>();
            params.put( ShapefileDataStoreFactory.URLP.key, file.toURI().toURL() );
            ShapefileDataStore ds = (ShapefileDataStore) new ShapefileDataStoreFactory().createNewDataStore(params);
            //定义图形信息和属性信息
            SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
            tb.setCRS(layer.getMetadata().getCrs());
            tb.setName("shapefile");
            tb.add("the_geom", Polygon.class);
            for(Field field:attrs.keySet()){
                tb.add(field.getName(), Class.forName(field.getType()));
            }
            ds.createSchema(tb.buildFeatureType());
            ds.setCharset(Charset.forName("utf-8"));
            //设置Writer
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(ds.getTypeNames()[0], Transaction.AUTO_COMMIT);
            //写下一条




            for(int n=0;n<re.size();n++){
                Geometry geo = re.get(n)._2.getGeometry();
                LinkedHashMap<Field,Object> attrs_n = re.get(n)._2.getAttributes();
                SimpleFeature feature = writer.next();
                feature.setAttribute("the_geom", geo);
                for(Field field:attrs_n.keySet()){
                    feature.setAttribute(field.getName(), attrs_n.get(field));
                }
            }
            writer.write();
            writer.close();
            ds.dispose();


        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
