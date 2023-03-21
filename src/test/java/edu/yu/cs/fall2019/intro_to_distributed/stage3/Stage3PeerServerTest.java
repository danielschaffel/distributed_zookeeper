package edu.yu.cs.fall2019.intro_to_distributed.stage3;

import edu.yu.cs.fall2019.intro_to_distributed.*;
import edu.yu.cs.fall2019.intro_to_distributed.stage1.Client;
import edu.yu.cs.fall2019.intro_to_distributed.stage1.ClientImpl;
import edu.yu.cs.fall2019.intro_to_distributed.stage3.*;

import java.util.List;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class Stage3PeerServerTest {
    // private static List<ZooKeeperPeerServer> servers = new ArrayList<>();

    // private final String goodClass = "public class Hello {\n" + "public void run() {\n"
    //         + " System.out.println(\"Hello\");\n" + "}\n" + "}";

    // // @BeforeClass
    // public static void setUpAndStartCluster() {
    //     // create IDs and addresses
    //     HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
    //     peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
    //     peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
    //     peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
    //     peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
    //     peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
    //     peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
    //     peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
    //     peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));

    //     for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
    //         HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
    //         map.remove(entry.getKey());
    //         ZooKeeperPeerServer server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(),
    //                 map);
    //         servers.add(server);
    //         new Thread(server, "Server on port " + server.getMyAddress().getPort()).start();
    //     }

    //     try {
    //         Thread.sleep(1000);
    //     } catch (InterruptedException e) {
    //         // TODO Auto-generated catch block
    //         e.printStackTrace();
    //     }

    //     servers.parallelStream().forEach(serv -> {
    //         System.out.println(serv.getId() + " " + serv.getPeerState() + " " + serv.getCurrentLeader().getCandidateID() );
    //     });
    // }

    // // @AfterClass
    // public static void tearDownCluster() {
    //     for (ZooKeeperPeerServer server : servers) {
    //         server.shutdown();
    //     }
    // }

    // @Test
    // public void testHappyPath() throws Exception {
    //     setUpAndStartCluster();

    //     URL url = new URL("http",
    //     "localhost",
    //     3000,
    //     "/compileandrun"
    //     );
        
    //     Thread.sleep(3000);
    //     HttpURLConnection con = (HttpURLConnection) url.openConnection();
    //     con.setDoOutput(true);
    //     OutputStream out = con.getOutputStream();
    //     out.write(goodClass.getBytes());
    //     out.flush();
    //     out.close();

    //     int res = con.getResponseCode();

    //     assertEquals(200, res);
    //     con.disconnect();

    //     tearDownCluster();
    // }
}