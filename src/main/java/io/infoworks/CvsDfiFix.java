package io.infoworks;

import java.util.*;

import java.io.*;
import java.lang.reflect.GenericArrayType;
import java.nio.file.*;
import java.nio.channels.FileChannel;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
//import com.sun.jersey.api.client.config.*;
//import com.sun.jersey.api.json.*;
//import com.sun.jersey.api.client.GenericType;
//import com.sun.jersey.api.client.config.DefaultClientConfig;

/*
import javax.xml.bind.annotation.XmlRootElement;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
*/

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by ashu on 1/13/17.
 *
 */
public class CvsDfiFix {
    private static Integer ingest_prop_error = -1;
    private static Integer ingest_num_not_match = -2;
    private static Integer file_exist_in_tmp_fatal = -3;

    public static void main(String[] args) {
        String type = null, sourceName = null, configPropPath = null;
        String inputDir = null;
        String srcDir = null;
        String serverAddr = null;
        String serverPort = null;
        String authToken = null;
        String archDir = null;
        String shellBin = null;
        String shellScript = null;
        String db = null;

        if (args.length != 3) {
            System.out.println("Need to provide table type, source name and path to config.properties");
            System.out.println("Usage: type sourceName path-to-config.properties");
            return;
        } else {
            type = args[0];
            sourceName = args[1];
            configPropPath = args[2];
            System.out.println(args[0]);
            System.out.println(args[1]);
            System.out.println(args[2]);
        }

        Properties props = new Properties();
        InputStream propertiesStream = null;
        try {
            propertiesStream = new FileInputStream(configPropPath);
            if (propertiesStream != null) {
                props.load(propertiesStream);
                serverAddr = props.getProperty("ServerAddress");
                serverPort = props.getProperty("ServerPort");
                inputDir = props.getProperty("InputDir");
                srcDir = props.getProperty("SrcDir");
                archDir = props.getProperty("ArchiveDir");
                authToken = props.getProperty("AuthToken");
                shellBin = props.getProperty("ShellBin");
                shellScript = props.getProperty("ShellScript");
                db = props.getProperty("Db");
                System.out.println("ServerAddress " + serverAddr);
                System.out.println("ServerPort " + serverPort);
                System.out.println("Inputdir " + inputDir);
                System.out.println("srcdir " + srcDir);
                System.out.println("archivedir " + archDir);
                System.out.println("auth_token " + authToken);
                System.out.println("shellBin " + shellBin);
                System.out.println("shellScript " + shellScript);
            } else {
                System.out.println("file not found");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (propertiesStream != null) {
                try {
                    propertiesStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        File dir = new File(inputDir);
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File file : directoryListing) {
                if (!file.isFile()) {
                    continue;
                }
                int fileCount=0;
                String timestamp = null;
                int res = checkFileCounts(file, type, srcDir);
                if (res == ingest_num_not_match ) {
                 //   System.out.println("ingestion file count did not match control file count");
                    continue;
                }
                if (res == file_exist_in_tmp_fatal ) {
                    System.out.println("ingestion file exists in source dir already");
                    return;
                }
                if (res == ingest_prop_error) {
                 //   System.out.println("Not the right file for ingestion type");
                    continue;
                }
                ProcessRestCalls processRestCalls = new ProcessRestCalls(type, sourceName, serverAddr, serverPort, authToken);
                String srcId = processRestCalls.getSourcesResponse();
                String tblGrpId = processRestCalls.getTableGrpsResponse(srcId);
                String jobId = processRestCalls.JobResponse(tblGrpId);
                boolean status = processRestCalls.getIngestionResponse(jobId);
                if (status) {
                    String fileName = file.getName();
                    String filenameparts[] = fileName.split("\\.");
                    timestamp = filenameparts[1];
                    String table = null;
                    if (db != null || !db.isEmpty()) {
                        table = String.format("%s.%s", db, type);
                    }
                    boolean verRes = verifyCounts(shellBin, shellScript, table, jobId, res, timestamp);
                    if (verRes) {
                        bookKeeping(fileName, srcDir, inputDir, archDir);
                    } else
                        errorOut(fileName, srcDir);
                } else {
                    errorOut(file.getName(), srcDir);
                }
            }
        } else {
            // Handle the case where dir is not really a directory.
            // Checking dir.isDirectory() above would not be sufficient
            // to avoid race conditions with another process that deletes
            // directories.
        }

    }

    private static int checkFileCounts(File file, String type, String srcDir) {
        BufferedReader reader = null;
        LineNumberReader readerL = null;
        String fileName = file.getName();
        String filenameparts[] = fileName.split("\\.");

        if (filenameparts.length == 3) // cnt file
            return ingest_prop_error;
    //    System.out.println("length " + filenameparts.length);
    //    System.out.println("file type " + filenameparts[0]);
        String fileType = filenameparts[0];
        String fileTs = filenameparts[1];

        if (!fileType.equalsIgnoreCase(type))
            return ingest_prop_error;

        String cntFile = String.format("%s.cnt", file.toString());
     //   System.out.println("cnt file " + cntFile);

        String text = null;
        int recordCount = 0;
        try {
            reader = new BufferedReader(new FileReader(new File(cntFile)));
            while ((text = reader.readLine()) != null) {
                recordCount = Integer.parseInt(text);
            }

            readerL = new LineNumberReader(new FileReader(file));
            int cnt = 0;
            String lineRead = "";
            while ((lineRead = readerL.readLine()) != null) {
                cnt = readerL.getLineNumber();
            }

            if (recordCount != cnt) {
                // error out
                return ingest_num_not_match;
            }

            boolean check = new File(srcDir, fileName).exists();
            if (check) {
                return file_exist_in_tmp_fatal;
            }

            File destFile = new File(new String(srcDir+fileName));
            addTimestampsToFile(file, destFile, fileTs);

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (readerL != null) {
                    readerL.close();
                }
            } catch (IOException e) {
            }

        }
        return recordCount;
    }

    private static void addTimestampsToFile(File source, File dest, String fileTs) throws IOException {
        BufferedReader reader = null;
        OutputStream os = null;
        try {
           // is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            reader = new BufferedReader(new FileReader(source));
            String text = null, dtext = null;
            String fileDate = fileTs.substring(0,8);
            while ((text = reader.readLine()) != null) {
                dtext = String.format("%s|%s|%s%n", text, fileDate, fileTs); // new String(text+"|"+filedate+"|"+fileTs+"\n");
                os.write(dtext.getBytes());
            }
        } finally {
            reader.close();
            os.close();
        }
    }

    private static void bookKeeping(String file, String srcDir, String inputDir, String archDir) {
        System.out.println("Ingestion succeeded for file " + file);
        File destFile = new File(new String(srcDir+file));
        destFile.delete();

        File srcFile = new File(new String(inputDir+file));
        srcFile.renameTo(new File(new String(archDir+file)));
    }

    private static void errorOut(String file, String srcDir) {
        // log an error
        System.out.println("Ingestion failed for file " + file);
        File destFile = new File(new String(srcDir+file));
        destFile.delete();
    }

    private static boolean verifyCounts(String shellBin, String shellScript, String table, String jobId, int count, String ts) {
        boolean status = false;
        try {
            String cmd = String.format("%s %s %s %s %d %s", shellBin, shellScript, table, jobId, count, ts);
            Process p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
            int res = p.exitValue();
            if (res != 0) {
                System.out.println("Verify counts failed");
            } else {
                System.out.println("Verify counts succeeded");
                status = true;
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return status;
    }
}
