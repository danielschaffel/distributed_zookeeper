package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


import edu.yu.cs.fall2019.intro_to_distributed.ElectionNotification;
import edu.yu.cs.fall2019.intro_to_distributed.JavaRunnerImpl;
import edu.yu.cs.fall2019.intro_to_distributed.Message;
import edu.yu.cs.fall2019.intro_to_distributed.TCPMessageReceiver;
import edu.yu.cs.fall2019.intro_to_distributed.TCPMessageSender;
import edu.yu.cs.fall2019.intro_to_distributed.UDPMessageReceiver;
import edu.yu.cs.fall2019.intro_to_distributed.UDPMessageSender;
import edu.yu.cs.fall2019.intro_to_distributed.Message.MessageType;

/*
IF I am the leader then we are going to pretend that the REAL server is the last server to 
join and that everyone already updated their votes to the highest vote.
ELSE REAL server is the leader but again the last one to join so everyone sends second highest vote
and then responds to leader with highest vote
*/
public class ZooKeeperPeerServerTestImpl implements Runnable {
    private static final int RTT = 1000 * 2;// ROUND Trip time = 2 seconds

    private final String javaClass = "public class Hello {" + "public void run(){"
            + "System.out.println(\"hello from mock\");" + "System.err.println(\"hello from err in mock\");" + "}"
            + "}";

    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> incomingHeartbeats;
    private LinkedBlockingQueue<Message> jobs;
    private LinkedBlockingQueue<Message> finishedJobs;

    private LinkedBlockingQueue<Message> followerJobs;
    private LinkedBlockingQueue<Message> followerFinishedJobs;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private TCPMessageSender jobSender;
    private TCPMessageReceiver jobReceiver;

    private TCPMessageSender followerJobSender;
    private TCPMessageReceiver followerJobReciever;

    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;

    private boolean isLeader;
    private final int myPort;
    private final InetSocketAddress myAddress;
    private final InetSocketAddress realServer;
    private volatile boolean shutdown = false;

    private Election election;
    private Leader leader;
    private Slave slave;
    private HeartBeater hbReader;

    private Logger logger;

    private int heartbeat;
    private int serverID;
    private ServerSocket iAmLeaderSocket;
    private ServerSocket iAmGateWaySocket;
    private Socket gatewaySocket;
    private Socket followerSocket;

    private HashSet<Long> deadServers;
    private int epoch = 0;

    public ZooKeeperPeerServerTestImpl(Map<Long, InetSocketAddress> peerIDtoAddress, InetSocketAddress realServer,
            int serverID, boolean isLeader, int mockPort) {
        this.peerIDtoAddress = (ConcurrentHashMap<Long, InetSocketAddress>) peerIDtoAddress;
        this.isLeader = isLeader;
        this.serverID = serverID;
        myPort = mockPort;
        myAddress = new InetSocketAddress("localhost", myPort);
        this.realServer = realServer;
        outgoingMessages = new LinkedBlockingQueue<>();
        incomingMessages = new LinkedBlockingQueue<>();
        incomingHeartbeats = new LinkedBlockingQueue<>();
        jobs = new LinkedBlockingQueue<>();
        finishedJobs = new LinkedBlockingQueue<>();
        followerJobs = new LinkedBlockingQueue<>();
        followerFinishedJobs = new LinkedBlockingQueue<>();
        senderWorker = new UDPMessageSender(outgoingMessages);
        receiverWorker = new UDPMessageReceiver(incomingMessages, incomingHeartbeats, myAddress, myPort);
        deadServers = new HashSet<>();
        election = new Election();
        leader = new Leader();
        slave = new Slave();
        hbReader = new HeartBeater();
        setUpLogger();
    }

    @Override
    public void run() {
        new Thread(senderWorker).start();
        new Thread(receiverWorker).start();
        // new Thread(jobSender).start();
        // new Thread(jobReceiver).start();
        new Thread(hbReader).start();
        Thread electionThread = new Thread(election);
        electionThread.start();
        while (!shutdown)
            ;
    }

    public void shutdown() {
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        if(jobSender != null) {
            jobReceiver.shutdown();
            jobSender.shutdown();
        }
        if(iAmLeaderSocket != null)
			try {
				iAmLeaderSocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        if(iAmGateWaySocket != null)
            try {
                iAmGateWaySocket.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
    }

    public void killServer(long serverID) {
        deadServers.add(serverID);
    }

    public void killLeader(long leaderID) {
        deadServers.add(leaderID);
        try {
            followerSocket.close();
            followerSocket = null;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void incrementEpoch() {
        epoch++;
    }

    public void iAmLeaderStuff() {
        try {
            if (iAmLeaderSocket == null)
                iAmLeaderSocket = new ServerSocket(myAddress.getPort());
            Socket sock = iAmLeaderSocket.accept();
            followerSocket = sock;

            // System.out.println("Got the servers socket");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Message work = new Message(MessageType.WORK, javaClass.getBytes(), myAddress.getHostName(), myPort,
                realServer.getHostName(), realServer.getPort(), 0);
        try {
            followerSocket.getOutputStream().write(work.getNetworkPayload());
            followerSocket.getOutputStream().flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        byte[] buffer = new byte[40960];
        Message fin = null;
        int i = 0;
        do {

            try {
                i = followerSocket.getInputStream().read(buffer);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } while (i <= 0);

        fin = new Message(buffer);
        System.out.println(new String(fin.getMessageContents()));
    }

    public void doElection(long leaderID) {
        if (isLeader) {
            election.sendLeaderVote(leaderID);
        } else {
            election.sendSecondHighestVote();
        }

        if (!isLeader) {
            election.readAndRespondToVotes();
            getLeaderConn();
            connectToLeader();
        }
    }

    public boolean sendMessageFromGatewayToLeaderAndGetResponse() {
        Message m = new Message(Message.MessageType.WORK, javaClass.getBytes(), myAddress.getHostName(),
                myAddress.getPort(), "localhost", 8010, 0);
        try {
            jobs.put(m);
            log("Sent job to leader");
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Message mess = null;
        while (mess == null)
            mess = followerJobs.poll();
        InputStream stream = new ByteArrayInputStream(mess.getMessageContents());
        String res = null;
        try {
            res = "200 " + new JavaRunnerImpl().compileAndRun(stream);
        } catch (IllegalArgumentException | IOException e) {
            res = "400 " + e.getMessage();
        }
        m = new Message(MessageType.COMPLETED_WORK, res.getBytes(), myAddress.getHostName(), myAddress.getPort(),
                realServer.getHostName(), realServer.getPort());
        try {
            followerFinishedJobs.put(m);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }

        Message finished = null;
        while(finished == null) finished =  finishedJobs.poll();
        // System.out.println("from gateway");
        // System.out.println(new String(finished.getMessageContents()));
        return true;
    }

    public boolean lotsOfMessagesFromGatewayToLeaderWithResponses(int numOfReqs) {
        boolean isTrue = true;
        for (int i = 0; i < numOfReqs && isTrue; i++) {
            isTrue = sendMessageFromGatewayToLeaderAndGetResponse();
        }
        return isTrue;
    }
    private void getLeaderConn() {
        if (iAmGateWaySocket == null) {
            try {
                iAmGateWaySocket = new ServerSocket(8000);
                gatewaySocket = iAmGateWaySocket.accept();
                jobSender = new TCPMessageSender(jobs, gatewaySocket);
                jobReceiver = new TCPMessageReceiver(finishedJobs, gatewaySocket);
                new Thread(jobSender).start();
                new Thread(jobReceiver).start();
                log("Created all of the TCP STUFF");
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void connectToLeader() {
        try {
            followerSocket = new Socket("localhost", 8080, myAddress.getAddress(), myPort);
            followerJobReciever = new TCPMessageReceiver(followerJobs, followerSocket);
            followerJobSender = new TCPMessageSender(followerFinishedJobs, followerSocket);
            new Thread(followerJobReciever).start();
            new Thread(followerJobSender).start();

        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    class Election implements Runnable {

        @Override
        public void run() {
            while (!shutdown) {
                
            }
        }
        
        void sendLeaderVote(long leaderID) {
            log("Sending leading votes");

            //FIRST HAVE TO SEND LOOKING
            for (Long id : peerIDtoAddress.keySet()) {
                if(id == serverID || deadServers.contains(id)) continue;
                char status = 'O';
                byte[] electionNotification = creatElectionNotification(id, leaderID, status);
                Message m = new Message(MessageType.ELECTION, electionNotification, myAddress.getHostName(), myPort,
                        realServer.getHostName(), realServer.getPort(), id);
                try {
                    outgoingMessages.put(m);
                    log(id + " sending vote to server");
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block 
                    e.printStackTrace();
                }
            }

            //THEN SEND FOLLOWING OR LOOKING
            for (Long id : peerIDtoAddress.keySet()) {
                if(id == serverID || deadServers.contains(id)) continue;
                char status;
                if(id == leaderID) status = 'E';
                else status = 'F';
                byte[] electionNotification = creatElectionNotification(id, leaderID, status);
                Message m = new Message(MessageType.ELECTION, electionNotification, myAddress.getHostName(), myPort,
                        realServer.getHostName(), realServer.getPort(), id);
                try {
                    outgoingMessages.put(m);
                    log(id + " sending vote to server");
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        void sendSecondHighestVote() {
            log("Sending follower votes");
            for (Long id : peerIDtoAddress.keySet()) {
                if (id == serverID || deadServers.contains(id)) continue;
                byte[] electionNotification = creatElectionNotification(id, 7, 'O');
                Message m = new Message(MessageType.ELECTION, electionNotification, myAddress.getHostName(), myPort,
                        realServer.getHostName(), realServer.getPort(), id);
                try {
                    outgoingMessages.put(m);
                    log("Sent follower vote from " + id);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        // TODO HAVE TO FIX FOR SECOND ELECTION
        void readAndRespondToVotes() {
            Message m = null;
            while (m == null) {
                m = incomingMessages.poll();
            }
            log("Got here");

            // long id = m.getRequestID();
            for (Long id : peerIDtoAddress.keySet()) {
                if(deadServers.contains(id)) continue;
                byte[] electionNotification = creatElectionNotification(id, 8, 'F');
                Message e = new Message(MessageType.ELECTION, electionNotification, myAddress.getHostName(), myPort,
                        realServer.getHostName(), realServer.getPort(), id);
                try {
                    outgoingMessages.put(e);
                    log(id + " responded to an incoming vote from server");
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }

        byte[] creatElectionNotification(long id, long vote, char serverStatus) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            try {
                dos.writeLong(vote);
                dos.writeChar(serverStatus);
                dos.writeLong(id);
                dos.writeLong(epoch);// TODO this is hardcoded need to actually make an EPOCH
            } catch (Exception e) {
                e.printStackTrace();
            }
            return out.toByteArray();
        }
    }

    class Leader implements Runnable {

        @Override
        public void run() {

        }

    }

    class Slave implements Runnable {

        @Override
        public void run() {

        }

    }

    class HeartBeater implements Runnable {

        @Override
        public void run() {
            while (!shutdown) {
                while (!shutdown) {
                    for (Long id : peerIDtoAddress.keySet()) {
                        if(id == serverID || deadServers.contains(id)) continue;
                        try {
                            outgoingMessages.put(new Message(MessageType.HEARTBEAT,
                                    Integer.toString(heartbeat).getBytes(), myAddress.getHostName(),
                                    myAddress.getPort(), realServer.getHostName(), realServer.getPort(), id));
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                    heartbeat++;
                    try {
                        Thread.sleep(RTT);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
              }
            }

        }
        
        private void sendBeat() {

        }
    }

    private void setUpLogger() {
        logger = Logger.getLogger("ZooKeeperServerTestImpl");
        File dir = new File("target/logs");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        FileHandler fh = null;
        try {
            fh = new FileHandler("target/logs/ZooKeeperServerTest");
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