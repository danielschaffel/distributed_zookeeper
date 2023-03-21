package edu.yu.cs.fall2019.intro_to_distributed.stage1;

import java.io.BufferedReader;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class ClientImpl implements Client {

    private String hostName;
    private int hostPort;
    public ClientImpl(String hostName, int hostPort) throws MalformedURLException {
        this.hostName = hostName;
        this.hostPort = hostPort;
    }

    public Response compileAndRun(String src) throws IOException {
        
        String protocol = "http";
        String handle = "/compileandrun";
        URL url = new URL(protocol, this.hostName, this.hostPort, handle);
        
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setDoOutput(true);
        OutputStream out =  con.getOutputStream();
        out.write(src.getBytes());
        out.flush();
        out.close();

        StringBuilder content;
        int res = con.getResponseCode();
        if(res == 200) {
            try(BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {

                String line;
                content = new StringBuilder();
    
                while((line = in.readLine()) != null) {
                    content.append(line);
                    content.append(System.lineSeparator());
                }
                in.close();
            }
        }
        else {
            try(BufferedReader in = new BufferedReader(new InputStreamReader(con.getErrorStream()))) {

                String line;
                content = new StringBuilder();
    
                while((line = in.readLine()) != null) {
                    content.append(line);
                    content.append(System.lineSeparator());
                }
                
                in.close();
            }
        }
        
        con.disconnect();
        return new Response(res, content.toString());
    }


    public static void main(String[] args) {
        ClientImpl client = null;
        
        try{
            client = new ClientImpl("localhost", 3009);
        } catch(Exception e) {
            System.out.println(e);
        }
        Response res = null;
        String src = "package foo.bar;\n"
        + "public class  {\n"
        +  "public void run() {\n"
        +  "System.out.println(\"hello from run\");\n"
        +"}\n"
        + "}";
        try {
            res = client.compileAndRun(src);
        } catch(Exception e) {
            System.out.println(e);
        }
        System.out.println(res.getCode());
        System.out.println(res.getBody());

    }
}