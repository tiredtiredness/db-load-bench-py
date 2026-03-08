import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

public class Result {
    @JsonProperty("engine")            public String engine;
    @JsonProperty("db_type")           public String dbType;
    @JsonProperty("method")            public String method;
    @JsonProperty("experiment_config") public Map<String, Object> experimentConfig;
    @JsonProperty("method_config")     public Map<String, Object> methodConfig;
    @JsonProperty("metrics")           public Map<String, Object> metrics;

    public static Result build(String dbType, String method,
                               int rows, double elapsed, Integer batchSize) {
        Result r = new Result();
        r.engine = "Java";
        r.dbType = dbType;
        r.method = method;

        r.experimentConfig = new HashMap<>();
        r.experimentConfig.put("rows", rows);

        r.methodConfig = new HashMap<>();
        r.methodConfig.put("batch_size", batchSize);

        r.metrics = new HashMap<>();
        r.metrics.put("elapsed", elapsed);
        r.metrics.put("rps",     Math.round((rows / elapsed) * 10.0) / 10.0);

        return r;
    }

    public String toJson() throws Exception {
        return new ObjectMapper().writeValueAsString(this);
    }
}