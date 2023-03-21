package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPMessageReceiver implements Runnable {

    private volatile boolean shutdown = false;
    private LinkedBlockingQueue<Message> jobs;
    private Socket socket;
    private long id;
    public TCPMessageReceiver(LinkedBlockingQueue<Message> job, Socket socket) {
        this.jobs = job;
        this.socket = socket;
    }

    public TCPMessageReceiver(LinkedBlockingQueue<Message> job, Socket socket, long id) {
        this.jobs = job;
        this.socket = socket;
        this.id = id;
    }
    // public void shutdown() throws InterruptedException {
    //     throw new InterruptedException();
    // }

    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void run() {

        while(!this.shutdown) {
            byte[] buffer = new byte[40960];
            try {
                // if(socket == null) System.out.println("YOU FOOL!\n"); 
                if(socket.isConnected()){
                    int i = socket.getInputStream().read(buffer);
                    if(i > 0) {
                        jobs.put(new Message(buffer));
                    //     Message m = new Message(buffer);
                    //     if(this.id != 0 && this.id < 8070)
                    //         System.out.println("Recieved job " + m.getRequestID() + " in follower " + this.id);    
                    //     else
                    //         System.out.println("Got job in leader or gateway or client facing server");
                    } 
                }
            } 
            catch(NullPointerException e) {
                // Ignore NullPointerExceptions. 
                // SHould figure out sourcre of them though
            }
            catch (Exception  e) {
                // e.printStackTrace();
                // System.err.println("Connection failed in " + id);
            }
        }
    }
}