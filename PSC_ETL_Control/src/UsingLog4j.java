import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class UsingLog4j {
	
	private static Logger logger = Logger.getLogger(UsingLog4j.class);

	public static void main(String[] args) {
		
		PropertyConfigurator.configure("/config/log4j.properties");

		logger.info("This is an info message.");
		
		logger.debug("This is a debug message.");
//		
		System.out.println("This is a debug message");
	}
}