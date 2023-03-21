package edu.yu.cs.fall2019.intro_to_distributed.stage1;

import static org.junit.Assert.assertEquals;


import org.junit.*;


public class Stage1Test {
    @Test
    public void compileAndRunWithPackage() throws Exception {
        String code = "package foo.bar;\npublic class Hello {\npublic void run(){\nSystem.out.print(\"hello, from hello\");\n}\n}\n";
        SimpleServerImpl server = new SimpleServerImpl(3000);
        server.start();
        ClientImpl client = new ClientImpl("localhost", 3000);
        Client.Response response = client.compileAndRun(code);
        assertEquals(response.getCode(), 200);
        String expected = "System.err:\n[]\nSystem.out:\n[hello, from hello]\n";
        assertEquals(expected, response.getBody());
        System.out.println("compileAndRunWithPackage");
        System.out.println(response.getCode());
        System.out.println(response.getBody());
        server.stop();
    }

    @Test
    public void compileAndRunWithoutPackage() throws Exception {
        String code = "\npublic class Hello {\npublic void run(){\nSystem.out.print(\"hello, from hello\");\n}\n}\n";
        SimpleServerImpl server = new SimpleServerImpl(3001);
        server.start();
        ClientImpl client = new ClientImpl("localhost", 3001);
        Client.Response response = client.compileAndRun(code);
        assertEquals(response.getCode(), 200);
        String expected = "System.err:\n[]\nSystem.out:\n[hello, from hello]\n";
        assertEquals(expected, response.getBody());
        System.out.println("compileAndRunWithoutPackage");
        System.out.println(response.getCode());
        System.out.println(response.getBody());
        server.stop();
    }

    @Test 
    public void compileError() throws Exception {
        String code = "package foo.bar;\npublic class  {\npublic void run(){\nSystem.out.print(\"hello, from hello\");\n}\n}\n";
        SimpleServerImpl server = new SimpleServerImpl(3002);
        server.start();
        ClientImpl client = new ClientImpl("localhost", 3002);
        Client.Response response = client.compileAndRun(code);
        assertEquals(response.getCode(), 400);
        System.out.println("compileError");
        System.out.println(response.getCode());
        System.out.println(response.getBody());
        server.stop();
    }

    @Test
    public void runTimeError() throws Exception {
        String code = "public class Hello {\n"
        +"public void run() {\n"
        +"throw new RuntimeException(\"hello from error\");\n"
        +"}\n"
        +"}";
        SimpleServerImpl server = new SimpleServerImpl(3003);
        server.start();
        ClientImpl client = new ClientImpl("localhost", 3003);
        Client.Response response = client.compileAndRun(code);
        assertEquals(200, response.getCode());
        String expected = "System.err:\n[hello from error\n]\nSystem.out:\n[]\n";
        assertEquals(expected, response.getBody());
        System.out.println("runTimeError");
        System.out.println(response.getCode());
        System.out.println(response.getBody());
        server.stop();
    }

    @Test
    public void multipleClientTest () throws Exception {

        String code = "package foo.bar;\npublic class Hello {\npublic void run(){\nSystem.out.print(\"hello, from hello\");\n}\n}\n";
        SimpleServerImpl server = new SimpleServerImpl(3000);
        server.start();
        ClientImpl client = new ClientImpl("localhost", 3000);
        ClientImpl client2 = new ClientImpl("localhost", 3000);
        Client.Response response = client.compileAndRun(code);
        Client.Response response2 = client2.compileAndRun(code);
        assertEquals(response.getCode(), 200);
        String expected = "System.err:\n[]\nSystem.out:\n[hello, from hello]\n";
        assertEquals(expected, response.getBody());
        assertEquals(expected, response2.getBody());
        System.out.println("compileAndRunWithPackage");
        System.out.println(response.getCode());
        System.out.println(response.getBody());
        server.stop();
    }
}