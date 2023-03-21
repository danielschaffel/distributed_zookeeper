package edu.yu.cs.fall2019.intro_to_distributed;

import edu.yu.cs.fall2019.intro_to_distributed.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer.ServerState;

public class ZooKeeperElection {

    private final static int finalizeWait = 300;
    private final static int maxNotificationInterval = 6000;

    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServer server;
    private ElectionNotification electionLeader;
    private HashMap<Long, Vote> voters = new HashMap<>();
    private Logger logger;

    public ZooKeeperElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        electionLeader = new ElectionNotification(this.server.getCurrentLeader().getCandidateID(),
                ZooKeeperPeerServer.ServerState.LOOKING, this.server.getId(), this.server.getPeerEpoch());
    }

    public synchronized Vote getVote() {
        return new Vote(this.electionLeader.leader, this.electionLeader.peerEpoch);
    }

    public synchronized Vote lookForLeader() {

        log("Starting election");
        if (this.server.getPeerState() != ServerState.OBSERVING) {
            sendNotifications();
        }
        try {
            Thread.sleep(400);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        while (this.server.getPeerState() == ServerState.LOOKING 
        || server.getPeerState() == ServerState.OBSERVING) {
            ElectionNotification en = null;
            try {
                en = readNotification(getNotification());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if(this.server.getPeerState() == ServerState.OBSERVING) {
                // System.out.println("Current election leader is " + electionLeader.leader);
            }
            // System.out.println("Got a vote with vote for " + en.leader);
            switch (en.state) {
                case LOOKING:
                log("Got a looking vote");
                checkLookingVote(new Vote(en.leader, en.peerEpoch), en.sid);
                break;
                
            case OBSERVING:
            case FOLLOWING:
                log("Got a following vote from " + en.sid + " with a vote for " + en.leader);
                this.voters.put(en.sid, new Vote(en.leader, en.peerEpoch));
                break;
            case LEADING:
                log("Got a leading vote from " + en.sid);
                checkOtherVote(new Vote(en.leader, en.peerEpoch), en.sid);
                break;
            }

            if (haveEnoughVotes(voters, getVote())) {
                log("Got enough votes for " + electionLeader.leader);
                try {
                    this.wait(finalizeWait);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                do {
                    en = null;
                    en = readNotification(this.incomingMessages.poll());
                    if (en != null) {
                        // have to change to both..
                        if (en.state == ZooKeeperPeerServer.ServerState.LOOKING) {
                            checkLookingVote(new Vote(en.leader, en.peerEpoch), en.sid);
                        } else {
                            checkOtherVote(new Vote(en.leader, en.peerEpoch), en.sid);
                        }
                    }
                } while (en != null );

                if ( haveEnoughVotes(voters, getVote()) ) {
                    if(this.server.getPeerState() == ServerState.OBSERVING)
                        this.server.setPeerState(ServerState.OBSERVING);
                    else if (this.server.getId() == electionLeader.leader) this.server.setPeerState(ZooKeeperPeerServer.ServerState.LEADING);
                    else this.server.setPeerState(ZooKeeperPeerServer.ServerState.FOLLOWING);

                    this.server.setCurrentLeader(getVote());
                    return getVote();
                }
            }
        }
        return getVote();
    }

    /*
     * NOTIFY OTHER SERVERS THAT STARTING ELECTION
     */
    private boolean sendNotifications() {
        if(this.server.getPeerState() == ServerState.OBSERVING) return true;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        try {
            dos.writeLong(this.electionLeader.leader);
            dos.writeChar(this.server.getPeerState().getChar());
            dos.writeLong(this.server.getId());
            dos.writeLong(this.electionLeader.peerEpoch);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        log("Sending notifications from real server");
        server.sendBroadcast(Message.MessageType.ELECTION, out.toByteArray());

        return true;
    }

    private Message clearingNotifications() {
        try {
            return incomingMessages.poll(finalizeWait, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private Message getNotification() throws InterruptedException {
        Message m = firstAttemptAtGet();
        
        if (m != null) {
            // log("Got a election notification");
            return m;
        }
        sendNotifications();

        double i = 0;
        long waitingTime = 2;
        long waitTime = (long) Math.pow(waitingTime,  i);

        // exponential wait time while
        // polling the incomingMessages
        while (m == null) {
            try {
                m = incomingMessages.poll(waitTime, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (waitTime < maxNotificationInterval) waitTime = (long) Math.pow(waitingTime, ++i);
        }
        log("got here after all the waiting ");
        return m;
    }

    private Message firstAttemptAtGet() {
        Message m = null;
        for (int i = 0; i < 2; i++) {

            try {
                m = incomingMessages.poll(maxNotificationInterval, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(m != null) return m;
        }
        return null;
    }

    protected boolean newVoteSupersedesCurrent(long newId, long newEpoch, long curId, long curEpoch) {
        return (newEpoch > curEpoch) || (newEpoch == curEpoch) && (newId > curId);
    }

    protected boolean haveEnoughVotes(HashMap<Long,Vote> votes, Vote v) {
        int inFavor = 0;
        for (Map.Entry<Long,Vote> entry : votes.entrySet()) {
            if (v.equals(entry.getValue())){
                inFavor++;
            }
        }
        boolean haveWinningQuorum = this.server.getQuorumSize() <= inFavor;
        return haveWinningQuorum;
    }

    //prob should change method name as it doesnt totally describe its funtion now
    private void checkLookingVote(Vote v, long sid) {
        if(!checkIfValid(sid)) return;
        if(newVoteSupersedesCurrent(v.getCandidateID(), v.getPeerEpoch(), electionLeader.leader, electionLeader.peerEpoch)) {
            this.voters.put(sid,v);
            this.electionLeader.leader = v.getCandidateID();
            this.electionLeader.peerEpoch = v.getPeerEpoch();
            this.electionLeader.sid = sid;
            sendNotifications();
        }
        else if(v.equals(getVote())) {
            this.voters.put(sid,v);
        }
    }

    private void checkOtherVote(Vote v, long sid) {
        if(!checkIfValid(sid)) return;
       if(newVoteSupersedesCurrent(v.getCandidateID(), v.getPeerEpoch(), electionLeader.leader, electionLeader.peerEpoch)) {
           this.voters.put(sid, v);
       }
       this.voters.put(sid,v);
    }

    private boolean checkIfValid(long sid) {
        return this.server.isInCluster(sid);
    }


    private ElectionNotification readNotification(Message m) {
        if(m == null) {
          //  System.out.println("was null in server " + this.server.getId());
            return null;
        }
        
        byte[] buf = m.getMessageContents();
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(buf));
        long leader = 0; 
        ServerState state = null;
        long sid = 0;
        long peerEpoch = 0;
        try {
            leader = in.readLong();
            state = ServerState.getServerState(in.readChar());
            sid = in.readLong();
            peerEpoch = in.readLong();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // System.out.println(sid + " sent " + leader  + " to " + this.server.getId());
        ElectionNotification en =  new ElectionNotification(leader, state, sid, peerEpoch) ;
      
        return en;
    }

    public Map<Long,Vote> getVotes() {
        return this.voters;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    private void log(String s){
        if(logger != null) logger.info(s);
    }
    
}