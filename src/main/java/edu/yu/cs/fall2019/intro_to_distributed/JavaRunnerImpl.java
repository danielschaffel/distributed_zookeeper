package edu.yu.cs.fall2019.intro_to_distributed;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.regex.*;

import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.Diagnostic;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class JavaRunnerImpl implements JavaRunner {

    private Path path;
    private Path sourcePath;
    private Path classPath;
    private String fileName;
    private String packageName;

    public JavaRunnerImpl() throws IOException{
        this.path = Files.createTempDirectory(null);
    }

    public JavaRunnerImpl(Path path) {
        if(Files.exists(path))
            this.path = path;
        else
            this.path = createPath(path);
    }

    public String compileAndRun(InputStream in) throws IllegalArgumentException, IOException {
        String source = getSource(in);
        // System.out.println(source);
        this.storeSource(source);
        this.compileSource(this.sourcePath);
        try{
            return this.runClass();
        }
        catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private String getSource(InputStream in) {
        String seperator = System.getProperty("line.separator");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        return reader.lines().collect(Collectors.joining(seperator));
    }

    private void storeSource(String source) throws IOException {
        this.fileName = this.getClassName(source);
        this.packageName = this.getPackage(source);
        if(this.fileName == null ) {
            throw new IllegalArgumentException("Missing class Name");
        }
        if(packageName != null) {
            this.sourcePath = this.makePackage(packageName);
        }
        else
            this.sourcePath = this.path;
        

        this.sourcePath = Paths.get(new File(this.sourcePath.toString() + System.getProperty("file.separator") + fileName +".java").getAbsolutePath());
        Files.write(this.sourcePath, source.getBytes());
    }

    private void compileSource(Path sourcePath) {
        
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);
        StringBuilder errors = new StringBuilder();
        
        ArrayList<String> list = new ArrayList<>();
        list.add(this.sourcePath.toString());

        Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjectsFromStrings(list);
        CompilationTask task = compiler.getTask(null, fileManager, diagnostics, null, null ,units);
        if(!task.call()) {
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
                errors.append(String.format("Line number: %d Column number: %d URI: %s\n", diagnostic.getLineNumber(), diagnostic.getColumnNumber(),diagnostic.getSource().toUri()));
             }
             throw new IllegalArgumentException(errors.toString());
        }
         try {
            fileManager.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.classPath = this.path.resolve(this.sourcePath.getParent() + this.fileName + ".class");
    }

    private String runClass() throws MalformedURLException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException{
        URL classURL = this.path.toFile().toURI().toURL();
        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{classURL});
        Class<?> clazz = null;

        if(packageName != null)
            clazz = classLoader.loadClass(this.packageName + "." + this.fileName);
        else
            clazz = classLoader.loadClass(this.fileName);

        Method helloMain = clazz.getDeclaredMethod("run");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();

        PrintStream orgOut = System.out;
        PrintStream orgErr = System.err;

        synchronized(this.getClass()){
            System.setOut(new PrintStream(outputStream));
            System.setErr(new PrintStream(errStream));
    
            try{
                helloMain.invoke(clazz.newInstance());
            }
            catch(Exception e) {
                System.err.println(e.getCause().getMessage());
            }
    
            System.setOut(orgOut);
            System.setErr(orgErr);
        }

        return String.format("System.err:\n[%s]\nSystem.out:\n[%s]",errStream.toString(),outputStream.toString());
    }

    private Path createPath(Path path) {
        path.toFile().mkdirs();
        return path;
    }

    private String getClassName(String source) {
        Pattern pattern = Pattern.compile("(?<=\\n|\\A)(?:public\\s)?(class|interface|enum)\\s([^\\n\\s]*)");
        Matcher matcher = pattern.matcher(source);
        if(matcher.find() && matcher.group().length() > 13){
            return matcher.group(2);
        }
        return null;
    }

    /**
     * 
     * @param source
     * @return package name of the class
     */
    private String getPackage(String source) {
        Pattern pattern = Pattern.compile("package(\\s)*([a-zA-Z]\\w*)(\\.[a-zA-Z]\\w*)*;");
        Matcher matcher = pattern.matcher(source);
        if(matcher.find()) {
            return matcher.group(0).replaceFirst("package\\s*", "").replace(";", "");
        }
        return null;
    }

    /**
     * Creates the folder structure neccasery for code
     * @param packageName
     * @return Path to source file
     */
    private Path makePackage(String packageName) {
        String[] names = packageName.split("\\.");
        File f = new File(this.path.toString());
        for(int i = 0 ; i  < names.length; i++) {
            f = new File(f,names[i]);
            f.mkdir();
        }
        return Paths.get(f.getAbsolutePath());
    }
    public static void main(String[] args) throws IOException{
        JavaRunnerImpl test = new JavaRunnerImpl(Paths.get("/home/daniel/Desktop"));
        String code = "package foo.bar;\n"
        + "public class {\n"
        //+  "public void run() {\n"
        //+  "System.out.println(\"hello from run\");\n"
        //+"}\n"
        + "}";
        String source = test.compileAndRun(new ByteArrayInputStream(code.getBytes()));
        if(source != null)
            System.out.println(source);
    }
}