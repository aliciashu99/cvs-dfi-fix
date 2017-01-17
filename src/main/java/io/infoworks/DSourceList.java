package io.infoworks;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashu on 1/13/17.
 */
public class DSourceList {

    private String error;
    private List<DSource> result = new ArrayList<DSource>();

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
    public List<DSource> getResult() {
        return result;
    }

    public void setResult(List<DSource> res) {
        this.result.addAll(res);
    }

    static class DSource {
        private String id;
        private String name;
        private String sourceSubtype;
        private String type;


        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSourceSubtype() {
            return sourceSubtype;
        }

        public void setSourceSubtype(String subtype) {
            this.sourceSubtype = subtype;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
