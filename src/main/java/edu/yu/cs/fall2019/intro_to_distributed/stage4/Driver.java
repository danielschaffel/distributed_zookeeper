package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import edu.yu.cs.fall2019.intro_to_distributed.Vote;
import edu.yu.cs.fall2019.intro_to_distributed.ZooKeeperPeerServer;
import edu.yu.cs.fall2019.intro_to_distributed.stage3.ZooKeeperPeerServerImpl;

public class Driver {
    public static void main(String[] args) {
        long id = Long.parseLong(args[0]);
        tradeMessages(id);
    }

    private static void tradeMessages(long id) {
           // create IDs and addresses
           ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>(3);
           peerIDtoAddress.put(0L, new InetSocketAddress("localhost", 8000));
           peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
           peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
           peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
           peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
           peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
           peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
           peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        //    peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));
   
           ZooKeeperPeerServer s = null;
           InetSocketAddress address = peerIDtoAddress.remove(id);
           if(id == 0) s = new GatewayServer(address.getPort(), 0, id, peerIDtoAddress);
           else {
               s = new ZooKeeperPeerServerImpl(address.getPort(), 0, id, peerIDtoAddress);
           }
           new Thread(s, "Server on port " + address.getPort()).start();
           try {
               Thread.sleep(5000);
           } catch (InterruptedException e1) {
               // TODO Auto-generated catch block
               e1.printStackTrace();
           }
            Vote leader = s.getCurrentLeader();
  
    }
}