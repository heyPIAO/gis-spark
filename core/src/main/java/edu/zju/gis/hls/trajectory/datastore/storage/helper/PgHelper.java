package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import edu.zju.gis.hls.trajectory.datastore.storage.config.PgConfig;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Hu
 * @date 2020/7/6
 * PostgreSQL 常用操作类
 * TODO 待测
 **/
@Slf4j
@ToString(callSuper = true)
public class PgHelper extends JDBCHelperImpl<PgConfig> {

    public PgHelper(PgConfig config) {
        super(config);
    }

    public PgHelper() {
        this(new PgConfig());
    }


    @Override
    protected String dbUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s",
                this.config.getUrl(), this.config.getPort(), this.config.getDatabase());
    }

    @Override
    public String transformTableName(String tableName) {
        return String.format("%s.\"%s\"", this.config.getSchema(), tableName);
    }

    //TODO
    @Override
    public boolean hasNext() {
        return false;
    }
}
