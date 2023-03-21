package edu.yu.cs.fall2019.intro_to_distributed.stage3;

import edu.yu.cs.fall2019.intro_to_distributed.*;
import edu.yu.cs.fall2019.intro_to_distributed.stage3.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Stage3PeerServerTest
{
    // private String validClass = "package edu.yu.cs.fall2019.intro_to_distributed.stage1;\n\npublic class HelloWorld\n{\n    public void run()\n    {\n        System.out.print(\"Hello System.out world!\\n\");\n        System.err.print(\"Hello System.err world!\\n\");\n    }\n}\n";
    // private String validClassReponse = "System.err: \n" + "Hello System.err world!\n" + "System.out:\n" + "Hello System.out world!\n";

    // private LinkedBlockingQueue<Message> outgoingMessages;
    // private LinkedBlockingQueue<Message> incomingMessages;
    // private int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
    // //private int[] ports = {8010, 8020};
    // private int leaderPort = this.ports[this.ports.length - 1];
    // private int myPort = 9999;
    // private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    // private ArrayList<ZooKeeperPeerServerImpl> servers;

    // public Stage3PeerServerTest() throws Exception
    // {
    //     //step 1: create sender & sending queue
    //     this.outgoingMessages = new LinkedBlockingQueue<>();
    //     UDPMessageSender sender = new UDPMessageSender(this.outgoingMessages);
    //     //step 2: create servers
    //     createServers();
    //     //step2.1: wait for servers to get started
    //     Util.startAsDaemon(sender,"Sender thread");
    //     this.incomingMessages = new LinkedBlockingQueue<>();
    //     UDPMessageReceiver receiver = new UDPMessageReceiver(this.incomingMessages, this.myAddress,this.myPort);
    //     Util.startAsDaemon(receiver,"Receiver thread");

    //     try
    //     {
    //         Thread.sleep(3000);
    //     }
    //     catch (Exception e)
    //     {
    //     }
    //     printLeaders();
    //     //step 3: since we know who will win the election, send requests to the leader, this.leaderPort
    //     for(int i = 0; i < this.ports.length; i++)
    //     {
    //         String code = this.validClass.replace("world!","world! from code version " + i);
    //         sendMessage(code);
    //     }
    //     //step 4: validate responses from leader
        
    //     checkResponses();
    //     //step 5: stop servers
    //     stopServers();
    // }
    
    // private void printLeaders()
    // {
    //     for (ZooKeeperPeerServerImpl server : this.servers)
    //     {
    //         Vote leader = server.getCurrentLeader();
    //         if (leader != null)
    //         {
    //             System.out.println("Server on port " + server.getMyAddress().getPort() + " whose ID is " + server.getId() + " has the following ID as its leader: " + leader.getCandidateID() + " and its state is " + server.getPeerState().name());
    //         }
    //     }
    // }
    
    // private void stopServers()
    // {
    //     for (ZooKeeperPeerServerImpl server : this.servers)
    //     {
    //         server.shutdown();
    //     }
    // }
    
    // private void checkResponses() throws Exception
    // {
    //     String completeResponse = "";
    //     for (int i = 0; i < this.ports.length; i++)
    //     {
    //         System.out.println(this.outgoingMessages.size());
    //         Message msg = this.incomingMessages.take();
    //         String response = new String(msg.getMessageContents());
    //         completeResponse += "Response #" + i + ":\n" + response + "\n";
    //     }
    //     System.out.println(completeResponse);
    // }

    // private void sendMessage(String code) throws InterruptedException
    // {
    //     Message msg = new Message(Message.MessageType.WORK,
    //             code.getBytes(),
    //             this.myAddress.getHostString(),
    //             this.myPort,
    //             "localhost",
    //             this.leaderPort);
    //     this.outgoingMessages.put(msg);
    // }

    // private void createServers()
    // {
    //     //create IDs and addresses
    //     HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
    //     for (int i = 0; i < this.ports.length; i++)
    //     {
    //         peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
    //     }
    //     //create servers
    //     this.servers = new ArrayList<>(3);
    //     for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet())
    //     {
    //         HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
    //         map.remove(entry.getKey());
    //         ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
    //         servers.add(server);
    //         new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
    //     }
    // }
    // public static void main(String[] args) throws Exception
    // {
    //     System.out.println("in test 3");
    //     new Stage3PeerServerTest();
    // }

}