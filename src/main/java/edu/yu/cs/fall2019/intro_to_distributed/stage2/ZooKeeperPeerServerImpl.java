package edu.yu.cs.fall2019.intro_to_distributed.stage2;


import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;


import edu.yu.cs.fall2019.intro_to_distributed.*;
import edu.yu.cs.fall2019.intro_to_distributed.Message.MessageType;

public class ZooKeeperPeerServerImpl implements ZooKeeperPeerServer {

    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState serverState;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private long peerEpoch;
    private Long id;
    private Vote currentLeader;
    private HashMap<Long, InetSocketAddress> peerIDtoAddress;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, HashMap<Long, InetSocketAddress> peerIDtoAddress) {
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.peerIDtoAddress = peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost",this.myPort);
        this.serverState = ServerState.LOOKING;
        //This might be stupid but i am starting of with myself as leader just for the original sendNotification in Election
        setCurrentLeader(new Vote(this.id,0));
    }

    private Vote lookForLeader() {
        ZooKeeperElection election = new ZooKeeperElection(this, this.incomingMessages);
        return election.lookForLeader();
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
        UDPMessageReceiver receiver = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort);
        UDPMessageSender sender = new UDPMessageSender(outgoingMessages);
        Util.startAsDaemon(receiver, "reciever");
        Util.startAsDaemon(sender, "sender");
        try {
            while(!this.shutdown) {
                switch (getPeerState()) {
                    case LOOKING:
                        setCurrentLeader(lookForLeader());
                        break;
                    case LEADING:
                        break;
                    case FOLLOWING:
                        break;
                    
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
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
            outgoingMessages.add(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(),target.getPort(),this.id));
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
        return this.peerIDtoAddress.size()/2 + 1;
    }
}