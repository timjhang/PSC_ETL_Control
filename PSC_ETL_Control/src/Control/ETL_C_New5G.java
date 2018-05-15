package Control;

import java.util.Calendar;
import java.util.Date;

public class ETL_C_New5G {

	public static void execute() {
		
		Calendar c1 = Calendar.getInstance();
    	final String strTime = String.format("%1$tH%1$tM", c1);
    	
    	//  使用編號" 2"設定檔(BatchRunTimeConfig)
    	boolean isRun = ETL_C_BatchTime.isExecute(strTime, " 2");
    	if (!isRun) {
    		System.out.println("ETL_C_New5G skip");
    		return;
    	}
    	
    	System.out.println("####ETL_C_New5G Start " + new Date());
    	
    	// 巡視所有單位資料庫, 是否有需建立新一代Table 資料庫
    	
    	
    	
    	System.out.println("####ETL_C_New5G End " + new Date());
	}
	
}
