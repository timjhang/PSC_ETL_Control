package Control;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_C_Supervision {

	String ETL_Mail_Type = "";
	
	// ETL監控寫入mail批次
	public void execute() {
		
		System.out.println("####ETL_C_Supervision 寄發通知信  Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		if ("1".equals(ETL_Mail_Type)) {
			
		} else if ("2".equals(ETL_Mail_Type)) {
			
		}
		
		System.out.println("####ETL_C_Supervision 寄發通知信  End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
	}
	
	public String getETL_Mail_Type() {
		return ETL_Mail_Type;
	}

	public void setETL_Mail_Type(String eTL_Mail_Type) {
		ETL_Mail_Type = eTL_Mail_Type;
	}
	
}
