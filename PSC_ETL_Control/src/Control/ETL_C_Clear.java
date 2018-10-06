package Control;

import java.io.File;

public class ETL_C_Clear {

	public static void execute() {
	
		/** 清除GAML Server上舊檔案 **/
		// 清除5代用DB2產出檔
		File db2_5G_FileFolder = new File("C:/ETL/L_Script/LoadBack");
		if (db2_5G_FileFolder.isDirectory()) {
			
			File[] fileArray = db2_5G_FileFolder.listFiles();
			
			for (int i = 0; i < fileArray.length; i++) {
				File aFile = fileArray[i];
				
				String filaName = aFile.getAbsolutePath();
				if (aFile.isFile()) {
					if (aFile.delete()) {
						System.out.println(filaName + "  刪除成功！");
					} else {
						System.out.println(filaName + "  刪除失敗，請調查！");
					}
				} else {
					System.out.println(filaName + "  非檔案類型，未執行清除，請調查！");
				}
			}
		} else {
			System.out.println(db2_5G_FileFolder.getAbsolutePath() + "  非預定路徑，未執行清除，請調查！");
		}
		
		// 如果D槽掛在這一台
		File dbServerSet = new File("D:/");
		if (dbServerSet.isDirectory()) {
			System.out.println("具有D槽，應為DB所在Server！");
			System.out.println("進行相關DB Log刪除");
		}
		
		
		/** 清除ETL Server上舊檔案 **/
		
		
	}
	
	// 清除資料夾底下所有檔案, 不清除資料夾本身
	private static void clearFile(File folder) {
		
		if (folder.isDirectory()) {
			File[] fileArray = folder.listFiles();
			
			for (int i = 0; i < fileArray.length; i++) {
				deleteAllFiles(fileArray[i]);
			}
			
		} else {
			System.out.println(folder.getAbsolutePath() + "  非資料夾，無法進行資料夾清除，請調查！！");
		}
	}
	
	// 刪除所有檔案, 資料夾則保留
	private static void deleteAllFiles(File file) {
		
		// 若為檔案, 則刪除檔案
		if (file.isFile()) {
			String fileName = file.getAbsolutePath();
			if (file.delete()) {
				System.out.println(fileName + "  檔案已刪除");
			} else {
				System.out.println(fileName + "  檔案刪除失敗");
			}
			return;
		}
		
		// 若為路徑, 將路徑下的檔案
		if (file.isDirectory()) {
			File[] fileArray = file.listFiles();
			
			for (int i = 0; i < fileArray.length; i++) {
				deleteAllFiles(fileArray[i]);
			}
		}
	}
	
	public static void main(String[] argv) {
		
		File file = new File("C:/");
		
		System.out.println(file.isDirectory());
		
	}
	
}
