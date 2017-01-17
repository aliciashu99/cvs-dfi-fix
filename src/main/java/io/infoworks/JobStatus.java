package io.infoworks;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashu on 1/13/17.
 */
public class JobStatus {

    private String error;
    private JobResult result;

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public JobResult getResult() {
        return result;
    }

    public void setResult(JobResult res) {
        this.result = res;
    }

    static class JobResult {
        private String jobId;
        private int percentCompleted;
        private String status;

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String id) {
            this.jobId = id;
        }

        public int getPercentCompleted() {
            return percentCompleted;
        }

        public void setPercentCompleted(int percent) {
            this.percentCompleted = percent;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

}
