package edu.zju.gis.hls.trajectory.model;


import edu.zju.gis.hls.trajectory.analysis.model.Field;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.util.Map;

/**
 * collect features into a vector tile
 * Created by lan yu on 2017/6/23.
 */
public interface VectorTileBuilder {

    /**
     * Add a feature to the tile
     * @param layerName The name of the feature set
     * @param featureId The identifier of the feature within the feature set
     * @param geometryName The name of the geometry property
     * @param geometry The geometry value
     * @param properties The non-geometry attributes of the feature
     */
    boolean addFeature(String layerName, String featureId, String geometryName, Geometry geometry,
                       Map<Field, Object> properties);

    <T> T build() throws IOException;
}
