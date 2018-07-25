package Migration;

import Bean.ETL_Bean_DM_LogData;

public abstract class Migration  implements Runnable {
	
	ETL_Bean_DM_LogData logData;

	public Migration() {
		
	}
	
	public Migration(ETL_Bean_DM_LogData logData) {
		this.logData = logData;
	}
	
	abstract void migration_File();
	
	@Override
	public void run() {
		migration_File();
	}
}
