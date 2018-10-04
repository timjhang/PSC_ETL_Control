package Tool;

import Bean.ETL_Bean_LogData;

public abstract class ETL_Tool_Thread implements Runnable {

	protected ETL_Bean_LogData logData;

	public ETL_Tool_Thread() {

	}

	public ETL_Tool_Thread(ETL_Bean_LogData logData) {
		this.logData = logData;
	}
	
	protected abstract void thread_work();

	@Override
	public void run() {
		 thread_work();
	}
}
