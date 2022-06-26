package edu.zju.gis.hls.trajectory.analysis.model;

import java.util.LinkedHashMap;

public class Geometry extends Feature<org.locationtech.jts.geom.Geometry>{
    public Geometry(String fid, org.locationtech.jts.geom.Geometry geometry, LinkedHashMap<Field, Object> attributes) {
        super(fid, geometry, attributes);
    }
}
