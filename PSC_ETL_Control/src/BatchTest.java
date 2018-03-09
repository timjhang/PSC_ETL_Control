import java.util.Calendar;

import Control.ETL_C_BatchTime;

public class BatchTest {

	private String connectString;
	private String userName;
	private String password;
	
	public String getConnectString() {
		return connectString;
	}
	public void setConnectString(String connectString) {
		this.connectString = connectString;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	public void execute() {
		System.out.println("######### Master_test Start");
		
		System.out.println("媽~我在這裡 @ Master_test");
//		System.out.println(connectString);
//		System.out.println(userName);
//		System.out.println(password);
		
		Calendar c1 = Calendar.getInstance();
    	final String strTime = String.format("%1$tH%1$tM", c1);
		
		System.out.println(ETL_C_BatchTime.isExecute(strTime, "01"));
		
		System.out.println("######### Master_test End");
	}
	
}
