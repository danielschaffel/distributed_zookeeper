package edu.yu.cs.fall2019.intro_to_distributed.stage1;

public interface SimpleServer
{
    //public SimpleServerImpl(int port) throws IllegalArgumentException, IOException

    static void main(String[] args)
    {
        int port = 9000;
        if(args.length >0)
        {
            port = Integer.parseInt(args[0]);
        }
        SimpleServer myserver = null;
        try
        {
            myserver = new SimpleServerImpl(port);
            myserver.start();
        }
        catch(Exception e)
        {
            System.err.println(e.getMessage());
            myserver.stop();
        }
    }

    /**
     * start the server
     */
    void start();

    /**
     * stop the server
     */
    void stop();
}