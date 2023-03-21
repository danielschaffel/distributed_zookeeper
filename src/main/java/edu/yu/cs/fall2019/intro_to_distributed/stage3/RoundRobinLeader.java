package edu.yu.cs.fall2019.intro_to_distributed.stage3;

import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.*;
import edu.yu.cs.fall2019.intro_to_distributed.*;

public class RoundRobinLeader implements Runnable {
    private final static int pollTime = 30;

    private ClientFacingServer clientFacingServer;
    private ServerSocket masterServer;
    private InetSocketAddress address;
    private ConcurrentHashMap<Long, InetSocketAddress> peers;
    // private HashMap<Long, InetSocketAddress> peers;

    private LinkedBlockingQueue<Socket> slaveSockets;

    private LinkedBlockingQueue<Message> jobs;
    private LinkedBlockingQueue<Message> finishedJobs;

    private Socket gatewaySocket;
    private LinkedBlockingQueue<Message> jobsFromGateway = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Message> jobsToGateway = new LinkedBlockingQueue<>();
    // private ConcurrentHashMap<InetAddress, List<Message>> slaveToWorkGiven = new
    // ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, Socket> jobIDToSocket = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, Message> jobIDToMessage = new ConcurrentHashMap<>();
    private TCPMessageReceiver receiver;
    private TCPMessageSender sender;

    private static volatile boolean shutdown = false;

    private Logger logger;

    public RoundRobinLeader(InetSocketAddress address, Map<Long, InetSocketAddress> peers,
            LinkedBlockingQueue<Message> jobs, LinkedBlockingQueue<Message> finishedJobs) {
        this.address = address;
        this.peers = (ConcurrentHashMap<Long, InetSocketAddress>) peers;
        this.slaveSockets = new LinkedBlockingQueue<>();
        this.jobs = jobs;
        this.finishedJobs = finishedJobs;
        // this.peers = (HashMap<Long, InetSocketAddress>) peers;
    }

    public void acceptConnections() {

        try {
            masterServer = new ServerSocket(address.getPort());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
            log("Trying again");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            acceptConnections();
            return;
        }

        // getSlaveConnectionsT();
        gatewaySocket = getGatewayConnection();
        new Thread(() -> {
            getSlaveConnectionsT();
        }).start();

        receiver = new TCPMessageReceiver(jobs, gatewaySocket, this.address.getPort());
        sender = new TCPMessageSender(finishedJobs, gatewaySocket);

        Util.startAsDaemon(receiver, "gateway receiver");
        Util.startAsDaemon(sender, "gateway sender");

        new Thread(() -> {
            giveOutWork();
        }).start();

        return;
    }

    private LinkedBlockingQueue<Socket> getSlaveConnections() {
        LinkedBlockingQueue<Socket> sockets = new LinkedBlockingQueue<>();
        for (int i = 0; i < this.peers.size() - 1; i++) {
            Socket s = null;
            try {
                s = masterServer.accept();
                // System.out.println("Got connection " + i);
            } catch (IOException e) {
                // TODO Auto-generated catch block
            }

            if (s != null) {
                sockets.add(s);
            }
        }
        return sockets;
    }

    private void getSlaveConnectionsT() {
        // System.out.println("The map size is " + this.peers.size());
        for (int i = 0; i < this.peers.size() - 1; i++) {
            Socket s = null;
            try {
                s = masterServer.accept();
                log("Accepted connection from " + s.getPort());
                // System.out.println("Got connection " + i);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                // e.printStackTrace();
                log("Failed to create a connection");
            }
            if(s != null) {
                slaveSockets.add(s);
            }
            else {
                i--;
            }
        }
        try {
            masterServer.close();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

    }

    private Socket getGatewayConnection() {
        Socket s = null;
        try {
            while (s == null) {
                log("created gateway connection");
                s = new Socket("localhost", 8000);
            }
        } catch (Exception e) {
            // TODO: handle exception
            // System.out.pri5ntln("OOPs");
            log(e.getMessage());
            try {
                Thread.sleep(10);

            } catch (Exception e1) {

            }
            // System.out.println("Trying again");
            return getGatewayConnection();
            // e.printStackTrace();
        }
        return s;
    }

    private void giveOutWork() {
        while (!shutdown) {
            Message m = null;
            m = jobs.poll();
            if (m != null) {
                log("Got job ");
                Message job = new Message(m.getMessageType(), m.getMessageContents(), m.getSenderHost(),
                        m.getSenderPort(), m.getReceiverHost(), m.getReceiverPort(), m.getRequestID());
                Socket s = slaveSockets.poll();
                if (s == null) {
                    // System.out.println("Null socket");
                    try {
                        jobs.put(job);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    continue;
                }
                // if(s.isClosed()) {
                // recoverWorkFromDeadSocket(s, m);
                // continue;
                // }

                try {
                    s.getOutputStream().write(job.getNetworkPayload());
                    s.getOutputStream().flush();
                    log("Sent job " + job.getRequestID() + " to " + s.getPort());
                    // System.out.println("Leader sent job " + job.getRequestID() + " to port " +
                    // s.getPort());
                } catch (IOException e) {
                    // TODO: handle exception
                    // e.printStackTrace();
                    try {
                        jobs.put(job);
                    } catch (InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    recoverWork();
                    // recoverWorkFromDeadSocket(s, m);
                    continue;
                    }

                    // if(!slaveToWorkGiven.contains(s.getInetAddress())) {
                    //     slaveToWorkGiven.put(s.getInetAddress(), new ArrayList<>());
                    // }
                    // slaveToWorkGiven.get(s.getInetAddress()).add(m);
                    jobIDToSocket.put(job.getRequestID(), s);
                    jobIDToMessage.put(job.getRequestID(), job);
                    slaveSockets.add(s);
                    
                    Runnable t = new Runnable(){
                        
                        @Override
                        public void run() {
                            byte[] buffer = new byte[40960];
                            Message fin = null;
                            try {
                                int i;
                                do{

                                    i = s.getInputStream().read(buffer);
                                }while(i <= 0);
                            
                                fin = new Message(buffer);
                                // slaveToWorkGiven.get(s.getInetAddress()).remove(fin);
                                jobIDToMessage.remove(fin.getRequestID());
                                jobIDToSocket.remove(fin.getRequestID());
                                finishedJobs.put(fin);
                                log("finished job " + fin.getRequestID());
                                return;
                            } catch (Exception e) {
                                e.printStackTrace();
                            }  
                        }
                    };

                    new Thread(t).start();
                }
            }
            
        }

        private void recoverWork() {
            // System.out.println("Trying to recover work");
            for(Long id: jobIDToSocket.keySet()) {
                if(jobIDToSocket.get(id) == null || jobIDToSocket.get(id).isClosed()) {
                    jobIDToSocket.remove(id);
                    jobs.add(jobIDToMessage.remove(id));
                    // System.out.println("Work was recovered");
                }
            }
        }
        public void shutdown(){ 
            shutdown = true;
            // exec.shutdown();
            // clientFacingServer.stop();
        }

    @Override
    public void run() {
        this.acceptConnections();
    }

    protected void setLogger(Logger logger){ this.logger = logger;}
    private void log(String msg) {if(logger != null) logger.info(msg);}
    }