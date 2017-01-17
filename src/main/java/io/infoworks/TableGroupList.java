package io.infoworks;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashu on 1/14/17.
 */
public class TableGroupList {
    private String error;
    private List<TableGroup> result = new ArrayList<TableGroup>();

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
    public List<TableGroup> getResult() {
        return result;
    }

    public void setResult(List<TableGroup> res) {
        this.result.addAll(res);
    }

    static class TableGroup {
        private String id;
        private String name;
        private int tables;

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

        public int getTables() {
            return tables;
        }

        public void setTables(int tbl) {
            this.tables = tbl;
        }
    }

}
