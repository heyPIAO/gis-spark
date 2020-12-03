package edu.zju.gis.hls.trajectory.datastore.storage.writer.mysql;

import edu.zju.gis.hls.trajectory.datastore.storage.writer.LayerWriterConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @author Zhou
 * @date 2020/7/27
 * 将图层数据写出到 Mysql 数据库配置
 **/
@Getter
@Setter
@ToString
@NoArgsConstructor
public class MysqlLayerWriterConfig extends LayerWriterConfig{

    // 默认路径（sinkPath）：jdbc:mysql://localhost:3306/test_db?serverTimezone=Asia/Shanghai
    private String tablename;
    private String username; // 默认用户名：mysql
    private String password; // 默认密码：mysql

    public MysqlLayerWriterConfig(String tablename){
        this(tablename, "mysql", "mysql");
    }

    public MysqlLayerWriterConfig(String tablename, String username, String password){
        this("jdbc:mysql://localhost:3306/test_db?serverTimezone=Asia/Shanghai", tablename, username, password);
    }

    public MysqlLayerWriterConfig(String sinkPath, String tablename, String username, String password){
        super(sinkPath);
        this.tablename = tablename;
        this.username = username;
        this.password = password;
    }

}
