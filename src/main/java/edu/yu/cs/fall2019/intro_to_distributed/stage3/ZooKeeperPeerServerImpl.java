package edu.yu.cs.fall2019.intro_to_distributed.stage3;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.HashSet;

import com.sun.net.httpserver.*;

import java.util.Arrays;

import edu.yu.cs.fall2019.intro_to_distributed.*;
import edu.yu.cs.fall2019.intro_to_distributed.Message.MessageType;
import edu.yu.cs.fall2019.intro_to_distributed.stage4.Heartbeat;

public class ZooKeeperPeerServerImpl implements ZooKeeperPeerServer {

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
    private Heartbeat gossiper;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> incomingHeartbeats;
    private LinkedBlockingQueue<Message> jobs;
    private LinkedBlockingQueue<Message> finishedJobs;

    private RoundRobinLeader leader;
    private JavaRunnerFollower slave;

    private HttpServer server;

    private Logger logger;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress) {
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.incomingHeartbeats = new LinkedBlockingQueue<>();
        this.jobs = new LinkedBlockingQueue<>();
        this.finishedJobs = new LinkedBlockingQueue<>();

        this.peerIDtoAddress = (ConcurrentHashMap<Long, InetSocketAddress>) peerIDtoAddress;
        // this.peerIDtoAddress = (HashMap<Long, InetSocketAddress>) peerIDtoAddress;
        this.removedPeerIDs = new CopyOnWriteArraySet<>();
        this.myAddress = new InetSocketAddress("localhost", this.myPort);
        // this.setPeerState(ServerState.LOOKING);
        this.serverState = ServerState.LOOKING;
        setUpLogger();
    }

    private Vote lookForLeader() {
        this.leader = null;
        this.slave = null;
        ZooKeeperElection el = new ZooKeeperElection(this, this.incomingMessages);
        log("About to set election logger");
        el.setLogger(logger);
        Vote v = el.lookForLeader();
        log("Finished election");
        while (v == null) {
            v = el.lookForLeader();
        }
        return v;
    }

    public synchronized long getLeaderID() {
        return this.currentLeader.getCandidateID();
    }

    @Override
    public boolean isInCluster(Long id) {
        return peerIDtoAddress.containsKey(id);
    }

    @Override
    public void shutdown() {

        this.shutdown = true;
        this.receiverWorker.shutdown();
        this.senderWorker.shutdown();
        this.gossiper.shutdown();

        if (leader != null)
            leader.shutdown();
        if (slave != null)
            slave.shutdown();
    }

    @Override
    public void run() {

        this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.incomingHeartbeats, this.myAddress,
                this.myPort);
        this.senderWorker = new UDPMessageSender(outgoingMessages);
        Util.startAsDaemon(this.receiverWorker, this.getId() + " reciever");
        Util.startAsDaemon(this.senderWorker, this.getId() + " sender");
        this.gossiper = new Heartbeat(this.incomingHeartbeats, this.outgoingMessages, this.peerIDtoAddress,
                this.removedPeerIDs, this.myAddress, this.id);
        Util.startAsDaemon(this.gossiper, "Gossiper");
        if (this.getCurrentLeader() == null) {
            this.setCurrentLeader(new Vote(this.id, this.peerEpoch));
            this.setCurrentLeader(lookForLeader());
        }
        setUpServer();
        // setUpListenersSendersAndServer();
        try {
            Thread t = null;
            while (!this.shutdown) {
                switch (getPeerState()) {
                case LOOKING:
                    this.setCurrentLeader(lookForLeader());
                    break;
                case LEADING:
                    if (this.leader == null) {
                        this.leader = new RoundRobinLeader(this.myAddress, this.peerIDtoAddress, this.jobs,
                                this.finishedJobs);
                        this.leader.setLogger(logger);
                        t = new Thread(this.leader);
                        t.start();
                        // t.sleep(1000);
                    }
                    checkForElectionNotifications();
                    break;
                case FOLLOWING:
                    if (this.slave == null) {
                        this.slave = new JavaRunnerFollower(getPeerByID(getCurrentLeader().getCandidateID()),
                                this.myAddress, this.jobs, this.finishedJobs);
                        this.slave.setLogger(logger);
                        t = new Thread(this.slave);
                        t.start();
                        // t.sleep(1000);
                    }
                    if (leaderIsDead()) {
                        if (slave != null) {
                            this.slave.shutdown();
                            this.slave = null;
                            this.leader = null;
                        }
                        this.peerEpoch++;
                        this.setPeerState(ServerState.LOOKING);
                        this.setCurrentLeader(new Vote(this.id, this.peerEpoch));
                        Thread.sleep(1000 * 2);
                        // this.setCurrentLeader(lookForLeader());
                    } else {
                        checkForElectionNotifications();
                    }
                    break;

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkForElectionNotifications() {

        Message m = null;
        try {
            m = incomingMessages.poll(20, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if (m != null) {
            if (m.getMessageType() != MessageType.ELECTION) {
                try {
                    outgoingMessages.put(m);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out);
                try {
                    dos.writeLong(this.currentLeader.getCandidateID());
                    dos.writeChar(this.getPeerState().getChar());
                    dos.writeLong(this.getId());
                    dos.writeLong(this.peerEpoch);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    outgoingMessages.put(new Message(Message.MessageType.ELECTION, out.toByteArray(),
                            myAddress.getHostName(), myPort, m.getSenderHost(), m.getSenderPort(), this.id));
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }
    }

    private boolean leaderIsDead() {
        if (this.getCurrentLeader() == null) {
            return true;
        }
        // System.out.println("Leader is " +
        // peerIDtoAddress.contains(this.getCurrentLeader()));
        return !this.peerIDtoAddress.containsKey(this.currentLeader.getCandidateID());
    }

    @Override
    public void setCurrentLeader(Vote v) {
        // System.out.println("Current Leader is " + v.getCandidateID());
        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
                // if(type == MessageType.ELECTION) {
                //     outgoingMessages.add(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(),
                // target.getPort(),(target.getPort() - 8000)/10 ));
                // }
                // else {
                    outgoingMessages.add(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(),
                            target.getPort(), this.id));
                // }
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        for (Long key : this.peerIDtoAddress.keySet()) {
            if (this.getId() != key)
                sendMessage(type, messageContents, this.peerIDtoAddress.get(key));
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.serverState;
    }

    @Override
    public void setPeerState(ServerState newState) {
        // synchronized(JavaRunner.class) {
        System.out.println(this.getId() + ": switching from " + this.getPeerState() + " to " + newState);
        // }
        if (newState == ServerState.FOLLOWING) {
            // System.out.println("Now following " + this.currentLeader.getCandidateID());
        }
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
        // This means there is odd amount because of Gateway
        if (this.peerIDtoAddress.size() % 2 == 0) {
            return this.peerIDtoAddress.size() / 2;
        } else {
            return this.peerIDtoAddress.size() / 2 + 1;
        }
        // return this.peerIDtoAddress.size()/2;
    }

    private void setUpServer() {
        try {
            this.server = HttpServer.create(new InetSocketAddress(this.myPort + 1), 0);
            HttpContext shutdownContext = server.createContext("/shutdown");
            HttpContext printGossip = server.createContext("/printgossip", new HttpHandler() {

                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    String table = gossiper.getGossip();
                    exchange.sendResponseHeaders(200, table.getBytes().length);
                    exchange.getResponseBody().write(table.getBytes());
                    exchange.getResponseBody().flush();
                    exchange.getResponseBody().close();
                }
            });

            HttpContext printGossipHistory = server.createContext("/gossipHistory", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException{
                    String history = getGossipHistory();
                    exchange.sendResponseHeaders(200, history.getBytes().length);
                    exchange.getResponseBody().write(history.getBytes());
                    exchange.getResponseBody().flush();
                    exchange.getResponseBody().close();
                }
            });
            // printGossip.setHandler(ZooKeeperPeerServerImpl::printGossip);
            shutdownContext.setHandler(ZooKeeperPeerServerImpl::shutdown);
            server.start();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void shutdown(HttpExchange exchange) {

    }

    private String getGossipHistory() {
        StringBuilder history = new StringBuilder();
        HashMap<Integer,String> gossips = this.gossiper.getGossipHistory();
        for(Integer str:gossips.keySet()) {
            history.append("Gossip recieved from " + (str-8000)/10 + "\n" + gossips.get(str) + "\n");
        }
        return history.toString();
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
