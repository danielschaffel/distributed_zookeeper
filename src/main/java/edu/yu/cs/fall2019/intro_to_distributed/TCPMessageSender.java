package edu.yu.cs.fall2019.intro_to_distributed;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.net.Socket;
public class TCPMessageSender implements Runnable {

    private LinkedBlockingQueue<Message> outgoingMessages;
    private Socket socket;
    private volatile boolean shutdown = false;

    public TCPMessageSender(LinkedBlockingQueue<Message> outgoingMessages, Socket socket) {
        this.outgoingMessages = outgoingMessages;
        this.socket = socket;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void run() {
        while(!this.shutdown) {
            try {
                Message m = this.outgoingMessages.poll(1, TimeUnit.SECONDS);
                
                if(m != null) {
                    this.socket.getOutputStream().write(m.getNetworkPayload());
                    this.socket.getOutputStream().flush();
                }
            } catch (Exception e) {
                //TODO: handle exception
            }
        }
    }

}