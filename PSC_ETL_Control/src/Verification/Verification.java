package Verification;

import Bean.ETL_Bean_LogData;

public abstract class Verification implements Runnable {

	ETL_Bean_LogData logData;
	
	public Verification() {
		
	}
	
	public Verification(ETL_Bean_LogData logData) {
		this.logData = logData;
	}
	
	abstract void verification_Datas();
	
	@Override
	public void run() {
		verification_Datas();
	}
	
}
