package io.infoworks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * Created by ashu on 1/17/17.
 */
public class ProcessRestCalls {

    private String type = null, sourceName = null;
    private String serverAddr = null;
    private String serverPort = null;
    private String authToken = null;

    public ProcessRestCalls(String type, String sourceName, String serverAddr, String serverPort, String authToken) {
        this.type = type;
        this.sourceName = sourceName;
        this.serverAddr = serverAddr;
        this.serverPort = serverPort;
        this.authToken = authToken;
    }

    protected String getSourcesResponse() {
        String id = null;
        try {
            Client client = Client.create();
            String srcUrl = String.format("http://%s:%s/v1.1/sources.json?auth_token=%s", serverAddr,serverPort, authToken);
            //       System.out.println("srcURL" + srcUrl);
            WebResource webResource = client.resource(srcUrl);
            ClientResponse response = webResource.get(ClientResponse.class);
            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }
            String output = response.getEntity(String.class);
            System.out.println(output);
            ObjectMapper mapper = new ObjectMapper();
            DSourceList srcObj = mapper.readValue(output, DSourceList.class);
            for (DSourceList.DSource source : srcObj.getResult()) {
                if (source.getName().equalsIgnoreCase(sourceName)) {
                    id = source.getId();
                    System.out.println("id " + id);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }

    protected String getTableGrpsResponse(String id) {
        String tblGrpId = null;
        try {
            Client client = Client.create();
            String tblGrpUrl = String.format("http://%s:%s/v1.1/source/table_groups.json?source_id=%s&auth_token=%s",
                    serverAddr, serverPort, id, authToken);
            WebResource webResource = client.resource(tblGrpUrl);
            ClientResponse response = webResource.get(ClientResponse.class);
            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }
            String output = response.getEntity(String.class);
            System.out.println(output);
            ObjectMapper mapper = new ObjectMapper();
            TableGroupList obj = mapper.readValue(output, TableGroupList.class);
            for (TableGroupList.TableGroup entry : obj.getResult()) {
                if (entry.getName().equalsIgnoreCase(type)) {
                    tblGrpId = entry.getId();
                    System.out.println("tblgrpid " + tblGrpId);
                    System.out.println("tbl name " + entry.getName());
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tblGrpId;
    }


    protected String JobResponse(String tblGrpId) {
        String jobId = null;
        try {
            Client client = Client.create();
            String postUrl = String.format("http://%s:%s/v1.1/source/table_group/ingest.json?table_group_id=%s&auth_token=%s",
                    serverAddr, serverPort, tblGrpId, authToken);
            System.out.println("postUrl " + postUrl);
            WebResource webResource = client.resource(postUrl);
            ClientResponse response = webResource.post(ClientResponse.class);
            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }
            String output = response.getEntity(String.class);
            System.out.println(output);
            ObjectMapper mapper = new ObjectMapper();
            JobList jobObj = mapper.readValue(output, JobList.class);
            jobId = jobObj.getResult().get(0);
            System.out.println("jobId " + jobId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobId;
    }

    protected boolean getIngestionResponse(String jobId) {
        boolean status = false;
        try {
            Client client = Client.create();
            String jobCheckUrl = String.format("http://%s:%s/v1.1/job/status.json?job_id=%s&auth_token=%s",
                    serverAddr, serverPort, jobId, authToken);
            WebResource webResource = client.resource(jobCheckUrl);
            while (true) {
                ClientResponse response = webResource.get(ClientResponse.class);
                if (response.getStatus() != 200) {
                    throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
                }
                String output = response.getEntity(String.class);
                System.out.println(output);

                ObjectMapper mapper = new ObjectMapper();
                JobStatus jobStatObj = mapper.readValue(output, JobStatus.class);
                String stat = jobStatObj.getResult().getStatus();
                if (stat.equalsIgnoreCase("completed")) {
                    status = true;
                    break;
                }
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return status;
    }

}
