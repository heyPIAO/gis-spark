package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import edu.zju.gis.hls.trajectory.analysis.index.KeyRangeFeature;
import edu.zju.gis.hls.trajectory.analysis.index.partitioner.PreBuildSpatialPartitioner;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import edu.zju.gis.hls.trajectory.analysis.util.GeometryUtil;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
@Getter
@ToString(callSuper = true)
@Slf4j
public class QuadTreePartitioner extends PreBuildSpatialPartitioner {
    private QuadTree quadTree;
    private QuadTreeIndexConfig conf;
    private CoordinateReferenceSystem crs = Term.DEFAULT_CRS;
    private Envelope extent = CrsUtils.getCrsEnvelope(this.crs);

    public QuadTreePartitioner(int partitionNum) {
        super(partitionNum);
    }
    public void setCrs(CoordinateReferenceSystem crs) {
        this.crs = crs;
        this.extent = CrsUtils.getCrsEnvelope(this.crs);
    }

    public void setConf(QuadTreeIndexConfig conf) {
        this.conf = conf;
        this.quadTree = new QuadTree(this.crs);
        this.isClip = conf.isClip();
    }
    @Override
    public <V extends Feature> void build(List<Tuple2<String, V>> samples) {
          this.quadTree.insert(samples.stream().map(g->(Tuple2<String, Feature>)g).collect(Collectors.toList()));
          this.quadTree.finish();
    }

    @Override
    public List<KeyRangeFeature> getKeyRangeFeatures(Geometry geometry) {
        List<Tuple2<String, Feature>> qs = quadTree.query(geometry);
        List<KeyRangeFeature> result = new ArrayList<>();
        if(qs.size()==0)
        {
            //jts的quadtree类 会出现 查询得到了结果 但实际上结果与目标全部不相交的情况
            qs.add(quadTree.generateDaumNode());
        }
        for(Tuple2<String, Feature> q: qs)
        {
            result.add(this.generateKeyRangeFeature(q._1,(Polygon) q._2.getGeometry().getEnvelope()));
        }
        return result;
    }

    @Override
    public KeyRangeFeature getKeyRangeFeature(String key) {
        if(key.equals(QuadTree.DAUM_KEY))
        {
            return new KeyRangeFeature(key,
                    GeometryUtil.envelopeToPolygon(CrsUtils.getCrsEnvelope(this.crs)),
                    getPartition(key));
        }
        return new KeyRangeFeature(
                key,
                (Polygon) quadTree.query(key)._2.getGeometry(),
                getPartition(key));
    }

    @Override
    public boolean equals(Object o) {
        return false;
    }
}
