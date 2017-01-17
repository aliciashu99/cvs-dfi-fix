package io.infoworks;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashu on 1/13/17.
 */
public class JobList {

    private String error;
    private List<String> result = new ArrayList<String>();

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
    public List<String> getResult() {
        return result;
    }

    public void setResult(List<String> res) {
        this.result.addAll(res);
    }

}
