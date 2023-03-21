package edu.yu.cs.fall2019.intro_to_distributed.stage4;

import org.junit.*;

import edu.yu.cs.fall2019.intro_to_distributed.stage3.ZooKeeperPeerServerImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNoException;

import java.beans.IntrospectionException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class Stage4Test {

    @Test
    public void testLeaderElected() throws InterruptedException {
        Thread.sleep(1000 * 5);
        ConcurrentHashMap<Long, InetSocketAddress> mockPeerIDtoAddress = new ConcurrentHashMap<>(3);
        // peerIDtoAddress.put(0L, new InetSocketAddress("localhost", 8000));
        mockPeerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        mockPeerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8010));
        mockPeerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8010));
        mockPeerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8010));
        mockPeerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8010));
        mockPeerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8010));
        mockPeerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8010));
        mockPeerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));

        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8010));
        // peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8000));

        ZooKeeperPeerServerTestImpl mock = new ZooKeeperPeerServerTestImpl(mockPeerIDtoAddress,
                mockPeerIDtoAddress.get(8l), 8,false, 8010);
        ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(8080, 0l, 8l, peerIDtoAddress);
        Thread m = new Thread(mock);
        Thread s = new Thread(server);
        s.start();
        m.start();
        mock.doElection(8);
        Thread.sleep(1000*2);
        assertEquals(8, server.getLeaderID());
        assertTrue(mock.lotsOfMessagesFromGatewayToLeaderWithResponses(9));
        mock.killServer(1);
        Thread.sleep(1000*15);
        mock.shutdown();
        server.shutdown();
        System.out.println("Shutdown ");
    }

    @Test
    public void testServerElectedLeader() throws InterruptedException {
        Thread.sleep(1000 * 5);
        ConcurrentHashMap<Long, InetSocketAddress> mockPeerIDtoAddress = new ConcurrentHashMap<>(3);
          // peerIDtoAddress.put(0L, new InetSocketAddress("localhost", 8000));
        mockPeerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8000));
        mockPeerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8000));
        mockPeerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8000));
        mockPeerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8000));
        mockPeerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8000));
        mockPeerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        mockPeerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8000));
        mockPeerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));

        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8000));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8000));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8000));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8000));
        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8000));
    //   peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8000));
        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8000));

        ZooKeeperPeerServerTestImpl mock = new ZooKeeperPeerServerTestImpl(mockPeerIDtoAddress,
                mockPeerIDtoAddress.get(6l), 6, true, 8000);
        ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(8060, 0l, 6l, peerIDtoAddress);
        Thread m = new Thread(mock);
        Thread s = new Thread(server);
        s.start();
        m.start();
        mock.doElection(8);
        Thread.sleep(1000);
        assertEquals(8, server.getLeaderID());
        Thread.sleep(1000*2);
        mock.iAmLeaderStuff();
        Thread.sleep(1000*2);
        
        mock.killLeader(8);
        Thread.sleep(1000 * 15);
        mock.incrementEpoch();
        mock.doElection(7);
        Thread.sleep(1000 * 3);
        assertEquals(7, server.getLeaderID());
        mock.iAmLeaderStuff();
        mock.shutdown();
        server.shutdown();
        System.out.println("Shutdown ");
    }
}