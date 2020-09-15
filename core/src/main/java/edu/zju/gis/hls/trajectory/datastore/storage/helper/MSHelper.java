package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.config.MSConfig;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * MySQL 常用操作类
 *
 * @author Hu
 * @date 2020/7/14
 * TODO 待测
 **/
@Slf4j
@ToString(callSuper = true)
public class MSHelper extends JDBCHelperImpl<MSConfig> {

    public MSHelper(MSConfig config) {
        super(config);
    }

    public MSHelper() {
        this(new MSConfig());
    }

    public boolean runSQL(String sql, Object... params) {
        log.info("SQL: " + sql);
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement(sql);
            for (int i = 1; i < params.length + 1; i++) {
                ps.setObject(i, params[i - 1]);
            }
            return ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper execute sql failed: " + sql);
        }
    }

    @Override
    public String transformTableName(String tableName) {
        return tableName;
    }

    @Override
    protected String dbUrl() {
        return String.format("jdbc:mysql://%s:%d/%s",
                this.config.getUrl(), this.config.getPort(), this.config.getDatabase());
    }

    //TODO
    @Override
    public boolean hasNext() {
        return false;
    }
}
