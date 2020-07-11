package edu.zju.gis.hls.trajectory.datastore.base;

import com.google.gson.Gson;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BaseEntity {

    private String id;

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public BaseEntity fromJson(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, this.getClass());
    }

}
