package edu.yu.cs.fall2019.intro_to_distributed;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.concurrent.LinkedBlockingQueue;

import edu.yu.cs.fall2019.intro_to_distributed.Message.MessageType;

public class UDPMessageReceiver implements Runnable
{
    private static final int MAXLENGTH = 4096;
    private final InetSocketAddress myAddress;
    private final int myPort;
    private LinkedBlockingQueue<Message> incomingMessages;
    private LinkedBlockingQueue<Message> incomingHeartbeats;
    private volatile boolean shutdown = false;

    public UDPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages, InetSocketAddress myAddress,int myPort)
    {
        this.incomingMessages = incomingMessages;
        this.myAddress = myAddress;
        this.myPort = myPort;
    }

    public UDPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> incomingHeartbeats, InetSocketAddress myAddress, int myPort) {
        this.incomingMessages = incomingMessages;
        this.incomingHeartbeats = incomingHeartbeats;
        this.myAddress = myAddress;
        this.myPort = myPort;
    }

    public void shutdown()
    {
        this.shutdown = true;
    }

    @Override
    public void run()
    {
        //create the socket
        DatagramSocket socket = null;
        try
        {
            socket = new DatagramSocket(this.myAddress);
            socket.setSoTimeout(3000);
        }
        catch(Exception e)
        {
            System.err.println("failed to create receiving socket");
            e.printStackTrace();
        }
        //loop
        while (!this.shutdown)
        {
            try
            {
                DatagramPacket packet = new DatagramPacket(new byte[MAXLENGTH], MAXLENGTH);
                socket.receive(packet); // Receive packet from a client
                Message received = new Message(packet.getData());
                if(received.getMessageType() == MessageType.HEARTBEAT || received.getMessageType() == MessageType.GOSSIP) {
                    this.incomingHeartbeats.put(received);
                }
                else if(received.getMessageType() == MessageType.ELECTION){
                    this.incomingMessages.put(received);
                }
            }
            catch(SocketTimeoutException ste)
            {
            }
            catch (Exception e)
            {
                if (!this.shutdown)
                {
                    e.printStackTrace();
                }
            }
        }
        //cleanup
        if(socket != null)
        {
            socket.close();
        }
    }
}