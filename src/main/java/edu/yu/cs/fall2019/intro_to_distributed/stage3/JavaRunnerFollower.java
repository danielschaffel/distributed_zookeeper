package edu.yu.cs.fall2019.intro_to_distributed.stage3;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import edu.yu.cs.fall2019.intro_to_distributed.JavaRunnerImpl;
import edu.yu.cs.fall2019.intro_to_distributed.Message;
import edu.yu.cs.fall2019.intro_to_distributed.TCPMessageReceiver;
import edu.yu.cs.fall2019.intro_to_distributed.TCPMessageSender;
import edu.yu.cs.fall2019.intro_to_distributed.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class JavaRunnerFollower implements Runnable {

    Socket masterConn = null;
    InetSocketAddress masterAddress;
    private InetSocketAddress myAddress;
    LinkedBlockingQueue<Message> jobs;
    LinkedBlockingQueue<Message> finishedJobs;
    TCPMessageReceiver receiver;
    TCPMessageSender sender;
    private static volatile boolean shutdown = false;

    private Logger logger;
    public JavaRunnerFollower(InetSocketAddress masterAddress, InetSocketAddress myAddress, LinkedBlockingQueue<Message> jobs, LinkedBlockingQueue<Message> finishedJobs) {
        this.masterAddress = masterAddress;
        this.myAddress = myAddress;
        this.jobs = jobs;
        this.finishedJobs = finishedJobs;
    }

    public void connectToLeader() {
        if (masterConn != null) {
            masterConn = null;
        }

        while(masterConn == null) {
        try {
            do{

                masterConn = new Socket(masterAddress.getAddress(), masterAddress.getPort(),this.myAddress.getAddress(),this.myAddress.getPort());
            }while (masterConn == null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            // System.out.println("Why is it not connecting in " + this.myAddress.getPort());
            // e.printStackTrace();
            try{

                Thread.sleep(10);
            }
            catch(Exception e1){

            }
            // try{
            //     connectToLeader();
            // } catch (StackOverflowError e2) {
            //     System.out.println("Server on port " + this.myAddress.getPort() + " could not connect to the leader");
            //     System.exit(0);
            // }
            // return;
        }
        }

        // System.out.println("About to start TCP Stuff");
        receiver = new TCPMessageReceiver(jobs, masterConn,this.myAddress.getPort());
        sender = new TCPMessageSender(finishedJobs, masterConn);
        Util.startAsDaemon(receiver, "JavaRunnerFollower reciever thread");
        Util.startAsDaemon(sender, "JavaRunnerFollower sender thread");

    }

    public void doWork() {

        // System.out.println("Starting doing work in " + this.myAddress.getPort());
        // System.out.println("Shutdown = " + this.shutdown);
        if(this.shutdown) this.shutdown = false;
        while (!shutdown) {
            Message m = jobs.poll();

            if (m != null)
            try {
                String res;
                try {
                    // System.out.println("Got job " + m.getRequestID() + " in follower");
                    log("Got job " + m.getRequestID());
                        res = new JavaRunnerImpl().compileAndRun(new ByteArrayInputStream(m.getMessageContents()));
                    } catch (IllegalArgumentException e) {
                        res = e.getLocalizedMessage();
                        res = "400 " + res;
                        Message ret = new Message(Message.MessageType.COMPLETED_WORK, res.getBytes(), m.getSenderHost(),
                                m.getSenderPort(), m.getReceiverHost(), m.getReceiverPort(), m.getRequestID());
                        try {
                            log("Finished job " + ret.getRequestID());
                            finishedJobs.put(ret);
                        } catch (InterruptedException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                        }
                        continue;
                    }
                    res = "200 " + res;
                    Message ret = new Message(Message.MessageType.COMPLETED_WORK, res.getBytes(), m.getSenderHost(),
                            m.getSenderPort(), m.getReceiverHost(), m.getReceiverPort(), m.getRequestID());
                    try {
                        log("Finished job " + ret.getRequestID());
                        finishedJobs.put(ret);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                } catch (IllegalArgumentException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            
        }
    }

    public void shutdown(){
        if(this.receiver != null) {

            this.receiver.shutdown();
            this.sender.shutdown();
            try {
                if(this.masterConn != null )
                    this.masterConn.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            this.masterConn = null;
        }
        shutdown = true;
    }

    @Override
    public void run() {
        this.connectToLeader();
        // System.out.println(this.myAddress.getPort() + " connected to the leader");
        this.doWork();
    }
    
    protected void setLogger(Logger logger){ this.logger = logger;}
    private void log(String msg) {if(logger != null) logger.info(msg);}
}