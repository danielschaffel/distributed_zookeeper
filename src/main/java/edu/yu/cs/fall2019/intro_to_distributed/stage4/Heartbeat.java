package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.Set;

import edu.yu.cs.fall2019.intro_to_distributed.Message;
import edu.yu.cs.fall2019.intro_to_distributed.Message.MessageType;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

public class Heartbeat implements Runnable {

    private static final int MYBEAT = 1;
    private static final int PEERBEAT = 0;
    private static final int RTT = 1000 * 2;// ROUND Trip time = 2 seconds
    private static final int TTR = 3;// time to remove a.k.a put in removed list
    private static final int TTD = 6;// time to die a.k.a be removedd from peerID list

    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> incomingHeartbeats;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private ConcurrentHashMap<Long, int[]> gossipList;
    private CopyOnWriteArraySet<Long> removedPeerIDs;
    private InetSocketAddress address;
    private int currentBeat;
    private long serverID;
    private HashMap<Integer,String> gossipHistory;

    public Heartbeat(LinkedBlockingQueue<Message> incomingHeartbeats, LinkedBlockingQueue<Message> outgoingMessages,
            Map<Long, InetSocketAddress> peerIDtoAddress, Set<Long> removedPeerIDs, InetSocketAddress address,
            long serverID) {
        this.incomingHeartbeats = incomingHeartbeats;
        this.outgoingMessages = outgoingMessages;
        this.shutdown = false;
        this.currentBeat = 0;
        this.address = address;
        this.serverID = serverID;
        this.peerIDtoAddress = (ConcurrentHashMap<Long, InetSocketAddress>) peerIDtoAddress;
        this.gossipList = new ConcurrentHashMap<>();
        this.removedPeerIDs = (CopyOnWriteArraySet<Long>) removedPeerIDs;
        this.gossipHistory = new HashMap<>();
        for (Long id : this.peerIDtoAddress.keySet()) {
            int[] arr = new int[2];
            gossipList.put(id, arr);
        }
    }

    @Override
    public void run() {

        try {
            Thread.sleep(400);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        new Thread(() -> {
            readBeatMessages();
        }).start();

        do {
            try {
                Thread.sleep(RTT);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            sendBeats();
            sendGossip();
            checkForDeadPeers();
            updateBeat();
            // printGossip();
        } while (!shutdown);
    }

    public void shutdown() {
        this.shutdown = true;
    }

    private void sendBeats() {
        // System.out.println("INSIDE sending beats with ID: " + this.currentBeat);
        for (Long id : this.peerIDtoAddress.keySet()) {
            outgoingMessages.add(sendBeat(MessageType.HEARTBEAT, this.peerIDtoAddress.get(id).getHostName(),
                    this.peerIDtoAddress.get(id).getPort()));
        }
    }

    private Message sendBeat(Message.MessageType type, String recieverHost, int receiverPort) {
        return new Message(type, Integer.toString(this.currentBeat).getBytes(), this.address.getHostName(),
                this.address.getPort(), recieverHost, receiverPort, serverID);
    }

    private void sendGossip() {
        int size = peerIDtoAddress.keySet().size();
        if(size <= 0) this.shutdown();
        int rand = new Random().nextInt(peerIDtoAddress.keySet().size());
        int i = 0;
        Long peer = 0l;
        for (Long key : peerIDtoAddress.keySet()) {
            if (i == rand) {
                peer = key;
                break;
            }
            i++;
        }

        String gossip = getGossip();
        InetSocketAddress peerAddress = peerIDtoAddress.get(peer);
        try {
            outgoingMessages.put(new Message(MessageType.GOSSIP, gossip.getBytes(), this.address.getHostName(),
                    this.address.getPort(), peerAddress.getHostName(), peerAddress.getPort(), this.serverID));
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            // e.printStackTrace();
        }
       return;
    }

    private synchronized void updateBeat() {
        this.currentBeat++;
    }
    private void readBeatMessages() {
        while(!shutdown) {
            Message msg = incomingHeartbeats.poll();
            // System.out.println("Just polled");
            if(msg == null) continue;
            // while((beat = incomingHeartbeats.poll()) == null);
            if(msg.getMessageType() == MessageType.HEARTBEAT) {
                readBeat(msg);
            }
            else if(msg.getMessageType() == MessageType.GOSSIP) {
                readGossip(msg);
            }
         
        }
    }

    private void readBeat(Message beat) {
        int peerbeat = Integer.parseInt(new String(beat.getMessageContents()));
        if(gossipList.get(beat.getRequestID())[PEERBEAT] < peerbeat) {
            gossipList.get(beat.getRequestID())[PEERBEAT] = peerbeat;
            gossipList.get(beat.getRequestID())[MYBEAT] = this.currentBeat;
        }
    }

    private void readGossip(Message gossipMessage) {
        String jsonMap = new String(gossipMessage.getMessageContents());
        this.gossipHistory.put(gossipMessage.getSenderPort(),jsonMap);
        Type type = new TypeToken<HashMap<Long,int[]>>() {}.getType();
     
        HashMap<Long,int[]> gossip = new Gson().fromJson(jsonMap, type);
    
    
        for(Long key : gossip.keySet()) {
            if(key == this.serverID) continue;
            if(!gossip.containsKey(key)) continue;
        
                synchronized(gossipList) {
                    int[] myArr = gossipList.get(key);
                    int[] arr = gossip.get(key);
                    if(myArr == null || arr == null) continue;
                    if(gossip.get(key)[PEERBEAT] > gossipList.get(key)[PEERBEAT]) {
                            System.out.printf("[%d]: updated [%d]'s heartbeat sequence to [%d] based on message from [%d] at node time [%d]\n",this.serverID,key,gossip.get(key)[PEERBEAT], gossipMessage.getRequestID(),this.currentBeat);
                            this.gossipList.get(key)[PEERBEAT] = gossip.get(key)[PEERBEAT];
    
                    }
                }
        } 
    }

    private void checkForDeadPeers() {
        for(Long id: gossipList.keySet()) {
            if(gossipList.get(id)[MYBEAT] + TTD < this.currentBeat) {
                this.peerIDtoAddress.remove(id);
                this.gossipList.remove(id);
                System.out.println(this.serverID + ": no heartbeat from server " + id + " - server failed");
            } else if(gossipList.get(id)[MYBEAT] + TTR < this.currentBeat) {
                this.removedPeerIDs.add(id);
                // System.out.println("Didnt get ping from " + id + " and it is being put in Removed list");
            }
        }
    }

  

    public String getGossip() {
        return new Gson().toJson(this.gossipList);
    }

    private void printGossip() {
        System.out.println(getGossip());
    }

    public HashMap<Integer,String> getGossipHistory() {
        return (HashMap<Integer,String>) gossipHistory.clone();
    }
}