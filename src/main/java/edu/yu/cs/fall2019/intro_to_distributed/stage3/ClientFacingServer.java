package edu.yu.cs.fall2019.intro_to_distributed.stage3;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.yu.cs.fall2019.intro_to_distributed.JavaRunnerImpl;
import edu.yu.cs.fall2019.intro_to_distributed.Message;

import com.sun.net.httpserver.*;


public class ClientFacingServer {

    private static int port;
    private  int masterPort;
    private static String hostname;
    private  HttpServer server;
    private  HttpServer masterSocket;
    private static LinkedBlockingQueue<Message> jobs;
    private  LinkedBlockingQueue<Message> finishedJobs; 
    // private static ConcurrentHashMap<String, HttpExchange> jobToExchange = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<Long, HttpExchange> jobToExchange = new ConcurrentHashMap<>();

    private volatile boolean shutdown = false;
    private volatile int id = 0; 
    // private static final ExecutorService exec = Executors.newFixedThreadPool(1000);
    private Runnable task;

    Logger logger;

    public ClientFacingServer(int port, int masterPort, String hostname) {
        this.port = port;
        this.masterPort = masterPort;
        this.hostname = hostname;
    }

    public ClientFacingServer(int port, int masterPort, String hostname, LinkedBlockingQueue<Message> q,
            LinkedBlockingQueue<Message> finished) {
        this.port = port;
        this.masterPort = masterPort;
        this.hostname = hostname;
        this.jobs = q;
        this.finishedJobs = finished;
    }

    public ClientFacingServer(int port) {
        this.port = port;
    }

    public void start() {

        try {
            this.server = HttpServer.create(new InetSocketAddress(this.port), 0);
            HttpContext runContext = server.createContext("/compileandrun", new HttpHandler() {

                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    compileAndRun(exchange);
                }

            });
            HttpContext printMap = server.createContext("/printmap");
            printMap.setHandler(ClientFacingServer::printMap);
            // runContext.setHandler(ClientFacingServer::compileAndRun);
            
            server.start();
            
            task = new Runnable(){

                @Override
                public void run() {
                    while(!shutdown) {
                        Message fin = finishedJobs.poll();
                        if(fin != null) {
                            // System.out.println("Finished job was received");
                            // System.out.println(fin.getRequestID() + " was removed with body " + new String(fin.getMessageContents()) + " with type" + fin.getMessageType());
                            HttpExchange ex;
                            if(jobToExchange.containsKey(fin.getRequestID()))
                                ex = jobToExchange.remove(fin.getRequestID());
                            else continue;
                            if(ex == null) continue;
                        
                            String[] pieces = new String(fin.getMessageContents()).split(" ",2);
                            try {
                                int code = Integer.parseInt(pieces[0]);
                                ex.sendResponseHeaders(code, pieces[1].getBytes().length);
                            } catch (IOException | NumberFormatException e) {
                                // TODO Auto-generated catch block
                                System.out.println("Error for message " + fin.getRequestID());
                                System.out.println(new String(fin.getMessageContents()));
                                e.printStackTrace();
                            }
    
                            OutputStream os = ex.getResponseBody();
                            try {
                                os.write(pieces[1].getBytes());
                                os.flush();
                                os.close();
                            } catch (IOException e) {
                                // TODO Auto-generated catch block
                                    System.out.println("What the HELL");
                                    e.printStackTrace();
                            }
                        }
                    }
                }
            };

            new Thread(task).start();
            // exec.submit(task);

        } catch (IOException e) {
            e.getStackTrace();
        }
    }

    public void stop() {
        shutdown = true;
        // exec.shutdown();
        server.stop(0);
        
    }

    private void compileAndRun(HttpExchange exchange) {
        String seperator = getSource(exchange.getRequestBody());
       
        Message m = makeMessage(exchange, seperator);
        if(exchange == null) System.out.println("What the actual fuck is happening");
        // System.out.println(m.getRequestID());
        jobToExchange.put(m.getRequestID(), exchange);
        // System.out.println(m.getRequestID() + " was put succesfully");
        jobs.add(m);
    }

    private static String getSource(InputStream in) {
        String seperator = System.getProperty("line.separator");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        return reader.lines().collect(Collectors.joining(seperator));
    }

    private Message makeMessage(HttpExchange ex, String source) {
        // System.out.println(id + " when creating message");
        int i = id++;
        Message m =  new Message(Message.MessageType.WORK, source.getBytes(), ex.getRemoteAddress().getHostName(), ex.getRemoteAddress().getPort(), hostname, port, i);
        log("Gateway received job " + m.getRequestID());
        return m;
    }

    private static void printMap(HttpExchange exchange) {
        String s = "";
        for(long job : jobToExchange.keySet()) {
            s += "Still have job " + job + " and its exchange is" + jobToExchange.get(job)  +"\n";
        }
        try {
            exchange.sendResponseHeaders(200, s.getBytes().length);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        OutputStream os = exchange.getResponseBody();
        try {
            os.write(s.getBytes());
            os.flush();
            os.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void setLogger(Logger logger){ this.logger = logger;}
    private void log(String msg) {if(logger != null) logger.info(msg);}
}