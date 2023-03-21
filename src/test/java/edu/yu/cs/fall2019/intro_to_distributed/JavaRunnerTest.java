package edu.yu.cs.fall2019.intro_to_distributed;

import static org.junit.Assert.assertEquals;

import java.io.*;


import org.junit.*;

public class JavaRunnerTest {

    @Test
    public void compileAndRunTest() throws IOException{
        String code = "package foo.bar;\npublic class Hello {\npublic void run(){\nSystem.out.println(\"hello, from hello \");\n}\n}\n";
        JavaRunnerImpl jri = new JavaRunnerImpl();
        String res = jri.compileAndRun((InputStream)new ByteArrayInputStream(code.getBytes()));
        assertEquals("System.err:\n[]\nSystem.out:\n[hello, from hello \n]", res);
        System.out.println("compileAndRunTest");
        System.out.println(res);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void compileError() throws IOException {
        String code = "package foo.bar;\npubli class Hello {\npublic void run(){\nSystem.out.println(\"hello, from hello \");\n}\n}\n";
        JavaRunnerImpl jri = new JavaRunnerImpl();
        jri.compileAndRun((InputStream) new ByteArrayInputStream(code.getBytes()));
    }

    @Test
    public void runTimeError() throws IOException {
        String code = "package foo.bar;\npublic class Hello {\npublic void run(){\nthrow new IllegalArgumentException(\"this  is the message\");\n}\n}";
        JavaRunnerImpl jri = new JavaRunnerImpl();
        String res = jri.compileAndRun((InputStream) new ByteArrayInputStream(code.getBytes()));
    }
}