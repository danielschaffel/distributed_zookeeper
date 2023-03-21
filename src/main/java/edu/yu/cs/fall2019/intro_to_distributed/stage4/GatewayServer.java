package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import edu.yu.cs.fall2019.intro_to_distributed.Message.MessageType;
import edu.yu.cs.fall2019.intro_to_distributed.stage3.ClientFacingServer;
import edu.yu.cs.fall2019.intro_to_distributed.*;
import com.sun.net.httpserver.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class GatewayServer implements ZooKeeperPeerServer {

    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState serverState;
    private long peerEpoch;
    private Long id;
    private Vote currentLeader;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    // private HashMap<Long, InetSocketAddress> peerIDtoAddress;
    private CopyOnWriteArraySet<Long> removedPeerIDs;

    private volatile boolean shutdown;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    private TCPMessageSender jobSenderToLeader;
    private TCPMessageReceiver jobReceiverFromLeader;

    private TCPMessageSender jobSenderToClient;
    private TCPMessageReceiver jobReceiverFromClient;
    
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> incomingJobs;
    private LinkedBlockingQueue<Message> outgoingJobs;
    private LinkedBlockingQueue<Message> incomingHeartbeats;

    private Socket leaderConn;
    private InetSocketAddress leaderAddress;
    private ClientFacingServer gatewayServer;
    private Heartbeat gossiper;
    private HttpServer server;

    private Logger logger;
    public GatewayServer(int port, long peerEpoch, long id,
    Map<Long, InetSocketAddress> peerIDtoAddress) {
            this.myPort = port;
            this.peerEpoch = peerEpoch;
            this.id = id;
            this.outgoingMessages = new LinkedBlockingQueue<>();
            this.incomingMessages = new LinkedBlockingQueue<>();
            this.incomingHeartbeats = new LinkedBlockingQueue<>();
            // this.peerIDtoAddress = (HashMap<Long, InetSocketAddress>) peerIDtoAddress;
            this.peerIDtoAddress = (ConcurrentHashMap<Long, InetSocketAddress>) peerIDtoAddress;
            this.removedPeerIDs = new CopyOnWriteArraySet<>();
            this.myAddress = new InetSocketAddress("localhost", this.myPort);
            // this.setPeerState(ServerState.OBSERVING);
            this.serverState = ServerState.OBSERVING;
            setUpLogger();
        }
        
        @Override
        public boolean isInCluster(Long id) {
            return peerIDtoAddress.containsKey(id);
        }
        
        @Override
        public void shutdown() {
            this.shutdown = true;
        }
        
    @Override
    public void run() {
            setUpServer();
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.incomingHeartbeats, this.myAddress, this.myPort);
            this.senderWorker = new UDPMessageSender(outgoingMessages);        
            this.incomingJobs = new LinkedBlockingQueue<>();
            this.outgoingJobs = new LinkedBlockingQueue<>();
            this.gossiper = new Heartbeat(this.incomingHeartbeats, this.outgoingMessages, 
            this.peerIDtoAddress, this.removedPeerIDs, this.myAddress, this.id);
            this.gatewayServer = new ClientFacingServer(3000, this.myAddress.getPort(), 
            this.myAddress.getHostName(), this.incomingJobs, this.outgoingJobs);
            this.gatewayServer.setLogger(logger);
            Util.startAsDaemon(this.receiverWorker, this.getId() + " reciever");
            Util.startAsDaemon(this.senderWorker, this.getId() + " sender");
            Util.startAsDaemon(this.gossiper, "Gossiper");
            this.gatewayServer.start();
            if (this.getCurrentLeader() == null) {
                setCurrentLeader(new Vote(this.id, this.peerEpoch));
                setCurrentLeader(lookForLeader());
            }
            try {
                while (!this.shutdown) {
                    switch (getPeerState()) {
                    case OBSERVING:
                        connectToLeader(this.peerIDtoAddress.get(getCurrentLeader().getCandidateID()));
                    }
                }
            } catch (Exception e) {
                // TODO: handle exception
                // System.out.println("Whar is wrong here");
            }
    }

    private void connectToLeader(InetSocketAddress address) {
   
        ServerSocket s = null;
        try {
             if (this.getCurrentLeader() == null) {
                //  this.peerEpoch++;
                setCurrentLeader(new Vote(this.id, this.peerEpoch));
                setCurrentLeader(lookForLeader());
        }
            // System.out.println("Creating Gateway");
            s = new ServerSocket(this.myAddress.getPort());
            leaderConn = s.accept();
            // System.out.println("Connected to new leader " + this.currentLeader.getCandidateID());
            // s.close();
            // System.out.println("In connect to leader");

    
            jobReceiverFromLeader = new TCPMessageReceiver(this.outgoingJobs, leaderConn, this.id);
            jobSenderToLeader = new TCPMessageSender(this.incomingJobs, leaderConn);
            Util.startAsDaemon(jobReceiverFromLeader, "job receiver from leader");
            Util.startAsDaemon(jobSenderToLeader, "Job Sender from Leader");
        } catch (Exception e) {
            //TODO: handle exception
            // System.out.println("Did it connect??");
        }
        while(!shutdown) {
            if(leaderIsDead()) {
                try {
                    // System.out.println("Why are we here");
                    this.leaderConn.close();
                    s.close();
                    jobReceiverFromLeader.shutdown();
                    // System.out.println("OBSERVErHAD LEADER DIE");
                    jobSenderToLeader.shutdown();
                    this.currentLeader = null;
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                this.peerEpoch++;
                this.setCurrentLeader(new Vote(this.id, this.peerEpoch));
                // System.out.println("Observe is about to start another election");
                this.setCurrentLeader(lookForLeader());
                // System.out.println("OBSERVER FINSIHED SECOND ELECTION");
                connectToLeader(this.peerIDtoAddress.get(this.currentLeader.getCandidateID()));
            }
        }
    }

    private boolean leaderIsDead() {
        return !this.peerIDtoAddress.containsKey(this.currentLeader.getCandidateID());
    }
    private Vote lookForLeader() {
        return new ZooKeeperElection(this, this.incomingMessages).lookForLeader();
    }

    @Override
    public void setCurrentLeader(Vote v) {
        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
            outgoingMessages.add(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(),target.getPort()));
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        for(Long key : this.peerIDtoAddress.keySet()) {
            if (this.getId() != key) sendMessage(type, messageContents, this.peerIDtoAddress.get(key));
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.serverState;
    }

    @Override
    public void setPeerState(ServerState newState) {
        // synchronized(JavaRunner.class) {
            System.out.println(this.getId() + ": switching from " + this.getPeerEpoch() + " to " + newState);
            if(newState == ServerState.FOLLOWING) {
                System.out.println("Now following server " + this.getCurrentLeader().getCandidateID());
            }
        // }
        this.serverState = newState;
    }

    @Override
    public Long getId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getMyAddress() {
        return this.myAddress;
    }

    @Override
    public int getMyPort() {
        return this.myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long id) {
        return peerIDtoAddress.get(id);
    }

    @Override
    public int getQuorumSize() {
        if(this.peerIDtoAddress.size() % 2 == 0) {
            return this.peerIDtoAddress.size()/2;
        } else {
            return this.peerIDtoAddress.size()/2 + 1;
        }
        // return this.peerIDtoAddress.size()/2 + 1;
    }
    private void setUpServer() {
        try {
            this.server = HttpServer.create(new InetSocketAddress(this.myPort + 1), 0);
            HttpContext shutdownContext = server.createContext("/shutdown");
            HttpContext printGossip = server.createContext("/printgossip", new HttpHandler(){
            
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    String table = gossiper.getGossip();
                    exchange.sendResponseHeaders(200, table.getBytes().length);
                    exchange.getResponseBody().write(table.getBytes());
                    exchange.getResponseBody().flush();
                    exchange.getResponseBody().close();
                }
            });

            HttpContext printStatuses = server.createContext("/printstatus", new HttpHandler(){

                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    StringBuilder str = new StringBuilder();
                    for(Long id : peerIDtoAddress.keySet()) {
                        if(removedPeerIDs.contains(id)) continue;
                        if(id == getCurrentLeader().getCandidateID()) {
                            str.append(id + ": LEADER\n");
                        }
                        else {
                           str.append(id +": FOLLOWER\n");
                        }
                    }
                    exchange.sendResponseHeaders(200, str.toString().getBytes().length);
                    exchange.getResponseBody().write(str.toString().getBytes());
                    exchange.getResponseBody().flush();
                    exchange.getResponseBody().close();
                }
            });
            // printGossip.setHandler(ZooKeeperPeerServerImpl::printGossip);
           
            HttpContext isThereLeader = server.createContext("/isthereleader", new HttpHandler() {

                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    if(currentLeader.getCandidateID() == id) {
                        String response = "false";
                        exchange.sendResponseHeaders(200, response.getBytes().length);
                        exchange.getResponseBody().write(response.getBytes());
                        exchange.getResponseBody().flush();
                        exchange.getResponseBody().close();
                    }
                    else {
                        String response = "true";
                        exchange.sendResponseHeaders(200, response.getBytes().length);
                        exchange.getResponseBody().write(response.getBytes());
                        exchange.getResponseBody().flush();
                        exchange.getResponseBody().close();
                    }
                }

            });
            server.start();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    private void setUpLogger() {
        logger = Logger.getLogger("ZooKeeperServerImpl" + this.myPort);
        File dir = new File("target/logs");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        FileHandler fh = null;
        try {
            fh = new FileHandler("target/logs/ZooKeeperServer" + this.myPort);
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        logger.setUseParentHandlers(false);
        logger.addHandler(fh);
        fh.setFormatter(new SimpleFormatter());
        logger.info("Configured Logger");
    }
    
    private void log(String message) {
        if(this.logger != null) {
            logger.info(message);
        }
    }
}