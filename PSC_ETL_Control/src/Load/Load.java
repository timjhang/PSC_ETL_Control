package Load;

import Bean.ETL_Bean_LogData;

public abstract class Load implements Runnable {

	ETL_Bean_LogData logData;
	String fedServer;
	String runTable;
	
	public Load() {
		
	}
	
	public Load(ETL_Bean_LogData logData, String fedServer, String runTable) {
		this.logData = logData;
		this.fedServer = fedServer;
		this.runTable = runTable;
	}
	
	abstract void load_File();
	
	@Override
	public void run() {
		load_File();
	}
	
}
