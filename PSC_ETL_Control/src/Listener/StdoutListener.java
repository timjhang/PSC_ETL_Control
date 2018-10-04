package Listener;
import java.io.PrintStream;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.apache.commons.logging.impl.LogFactoryImpl;

public class StdoutListener implements ServletContextListener {
public void contextDestroyed(ServletContextEvent event) {
   }
   private void log(Object info) {
       LogFactoryImpl.getLog(getClass()).info(info);
   }
   public void contextInitialized(ServletContextEvent event) {
       PrintStream printStream = new PrintStream(System.out) {
           public void println(boolean x) {
               log(Boolean.valueOf(x));
               System.err.println(Boolean.valueOf(x));
           }
           public void println(char x) {
               log(Character.valueOf(x));
               System.err.println(Character.valueOf(x));
           }
           public void println(char[] x) {
               log(x == null ? null : new String(x));
               System.err.println(x == null ? null : new String(x));
           }
           public void println(double x) {
               log(Double.valueOf(x));
               System.err.println(Double.valueOf(x));
           }
           public void println(float x) {
               log(Float.valueOf(x));
               System.err.println(Float.valueOf(x));
           }
           public void println(int x) {
               log(Integer.valueOf(x));
               System.err.println(Integer.valueOf(x));
           }
           public void println(long x) {
               log(x);
               System.err.println(x);
           }
           public void println(Object x) {
               log(x);
               System.err.println(x);
           }
           public void println(String x) {
               log(x);
               System.err.println(x);
           }
       };
       System.setOut(printStream);
//       System.setErr(printStream);
   }
}