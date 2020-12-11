package edu.zju.gis.hls.trajectory.analysis.index.quadtree;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Polygon;
import edu.zju.gis.hls.trajectory.analysis.util.CrsUtils;
import edu.zju.gis.hls.trajectory.analysis.util.GeometryUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import scala.Tuple2;
import scala.Serializable;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@ToString(callSuper = true)
public class QuadTree implements Serializable {
    public static String DAUM_KEY = "DAUM";

    private Quadtree si;

    @Getter
    private boolean isFinish;

    @Setter
    @Getter
    private CoordinateReferenceSystem crs;

    public QuadTree(CoordinateReferenceSystem crs)
    {
        this.si=new Quadtree();
        this.crs=crs;
        this.isFinish=false;
    }

    public void insert(List<Tuple2<String, Feature>> features) {
        for (Tuple2<String, Feature> feature: features) {
            this.insert(feature._2.getGeometry().getEnvelopeInternal(), feature._1, feature._2);
        }
    }

    public void insert(Envelope e, String index, Feature o)  {
        if (!this.isFinish) {
            this.si.insert(e, new Tuple2<String, Feature>(index, o));
        } else {
            log.warn("QuadTree has already built");
        }
    }

    public List<Tuple2<String, Feature>> query(Envelope e) {
        List<Tuple2<String, Feature>> result = (List<Tuple2<String, Feature>>) this.si.query(e);
        if (result.size() == 0) {
            result.add(generateDaumNode());
        }
        return result;
    }
    public Tuple2<String, Feature>  generateDaumNode() {
        return new Tuple2<String, Feature>(DAUM_KEY, new Polygon(GeometryUtil.envelopeToPolygon(CrsUtils.getCrsEnvelope(this.crs))));
    }
    public List<Tuple2<String, Feature>> query(Geometry g) {
        List<Tuple2<String, Feature>> e = this.query(g.getEnvelopeInternal());
        if (e.size() == 1 && e.get(0)._1.equals(QuadTree.DAUM_KEY)) return e;
        List<Tuple2<String, Feature>> r=e.stream()
                .filter(v -> v._2.getGeometry().intersects(g))
                .collect(Collectors.toList());
        return r;
    }

    /**
     * 用一个HashMap辅助key查询
     * @param key
     * @return
     */
    public Tuple2<String, Feature> query(String key) {
        List<Tuple2<String, Feature>> q = (List<Tuple2<String, Feature>>)this.si.queryAll();
        q = q.stream().filter(x->x._1.equals(key)).collect(Collectors.toList());
        if (q.size() == 0) q.add(this.generateDaumNode());
        return q.get(0);
    }

    public void finish() {
        this.isFinish = true;
    }
}
