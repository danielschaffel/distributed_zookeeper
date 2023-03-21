package edu.yu.cs.fall2019.intro_to_distributed.stage1;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;

import com.sun.net.httpserver.HttpServer;

import edu.yu.cs.fall2019.intro_to_distributed.JavaRunnerImpl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpContext;

public class SimpleServerImpl implements SimpleServer {

    private int port;
    private HttpServer server;
    public SimpleServerImpl(int port) throws IllegalArgumentException, IOException {
        this.port = port;
    }


    public void start() {

        try{
            this.server = HttpServer.create(new InetSocketAddress(this.port),0);
            HttpContext runContext = server.createContext("/compileandrun");
            runContext.setHandler(SimpleServerImpl::compileAndRun);
            server.start();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void stop() {
        this.server.stop(0);
    }


    private static void compileAndRun(HttpExchange exchange) {

        String res = null;
        try {
            try {
                res = new JavaRunnerImpl().compileAndRun(exchange.getRequestBody());
            } catch (Exception e) {
                exchange.sendResponseHeaders(400, e.getMessage().toString().length());
                OutputStream os = exchange.getResponseBody();
                os.write(e.getMessage().toString().getBytes());
                os.flush();
                os.close();
            }
            exchange.sendResponseHeaders(200, res.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(res.getBytes());
            os.flush();
            os.close();
            } 
            catch(Exception e) {
        }
    }

    public static void main(String[] args) {
        SimpleServerImpl server = null;
        try {
            server = new SimpleServerImpl(3000);
        } catch (Exception e) {
            System.out.println(e);
        }

        try{
            
            server.start();
        }
        catch(Exception e) {
        }
    }
}