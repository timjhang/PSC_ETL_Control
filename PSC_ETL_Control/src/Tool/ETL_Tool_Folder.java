package Tool;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;



public class ETL_Tool_Folder {
	
	public enum Type {
		ALLFile, ALLFileKeepFolder, OnlyDocument
	}
	
	private final static String[] central_nos = { "018", "600", "951", "952", "928", "910", "605" };
	
	private final static String ArchlogFilePath = "D:/archlog";
	private final static String LoadBackFilePath = "C:/ETL/LoadBack";
	private final static String GAMLDownLoadFilePath ="C:/ETL/DB";
	private final static String ETLDownLoadFilePath ="D:/ETL/DB";
	private final static String DMFilePath ="C:/ETL/DM";
	private final static int KEEP_DAY = 30;

	/**
	 * 取得資料
	 * @param folder 處理目錄
	 * @param type 處理種類
	 * @return 回傳資料
	 * @throws IOException
	 */
	public static List<File> listFilesForFolder(File folder, Type type) throws IOException {
		List<File> list = new ArrayList<File>();
		File[] fileEntrys = null;
		switch (type) {
		case ALLFile:
			fileEntrys = folder.listFiles();
			if (fileEntrys == null || fileEntrys.length == 0) {
				list.add(folder.getCanonicalFile());
				return list;
			}

			for (int i = 0; i < fileEntrys.length; i++) {
				File fileEntry = fileEntrys[i];
				if (fileEntry.isDirectory()) {
					list.addAll(listFilesForFolder(fileEntry, ETL_Tool_Folder.Type.ALLFile));
				} else {
					list.add(fileEntry);
				}

				if (i == fileEntrys.length - 1) {
					list.add(folder.getCanonicalFile());
				}
			}
			
			return list;
			
		case ALLFileKeepFolder:
			list = listFilesForFolder(folder, ETL_Tool_Folder.Type.ALLFile);
			list = list.subList(0, list.size() - 1);
			return list;
			
		case OnlyDocument:
			for (final File fileEntry : folder.listFiles()) {
				if (fileEntry.isDirectory()) {
					list.addAll(listFilesForFolder(fileEntry, ETL_Tool_Folder.Type.OnlyDocument));
				} else {
					list.add(fileEntry);
				}
				
			}

			return list;
			
		default:
			return null;
		}

	}

	/**
	 * 刪除檔案
	 * 
	 * @param list
	 */
	public static void dropFileByList(List<File> list) {
		for (File f : list) {
			f.delete();
		}
	}

	/**
	 * 刪除archlog資料夾下所有文件 133
	 * 
	 * @throws IOException
	 */
	private static void dropDBArchlog() throws IOException {
		File folder = new File(ETL_Tool_Folder.ArchlogFilePath);
		if(folder.exists()) {
			List<File> list = listFilesForFolder(folder, ETL_Tool_Folder.Type.OnlyDocument);
			ETL_Tool_Folder.dropFileByList(list);	
		}
	}

	/**
	 * 刪除LoadBack資料夾下所有文件 133
	 * 
	 * @throws IOException
	 */
	private static void dropLoadBack() throws IOException {
		File folder = new File(ETL_Tool_Folder.LoadBackFilePath);
		if (folder.exists()) {
			List<File> list = listFilesForFolder(folder, ETL_Tool_Folder.Type.OnlyDocument);
			ETL_Tool_Folder.dropFileByList(list);
		}
	}

	/**
	 * 刪除各單位 C:\ETL\DB910\DOWNLOAD 底下檔案 某個日期之前 ex:910MASTER_20180625_185224.txt 133
	 * 
	 * @param deleteBefDate
	 * @throws Exception
	 */
	private static void dropGAMLDownLoad(Date deleteBefDate) throws Exception {
		for (String central_no : central_nos) {
			//System.out.println(central_no);
			File folder = new File(ETL_Tool_Folder.GAMLDownLoadFilePath + central_no + "/DOWNLOAD");

			// 清除該目錄符合條件的子檔案 txt
			ETL_Tool_Folder.dropSubDocumentByCondition(folder, 10, 18, deleteBefDate, "Txt");
			ETL_Tool_Folder.dropSubDocumentByCondition(folder, 13, 21, deleteBefDate, "Txt");

		}
	}
	
	/**
	 * 刪除各單位 D:\ETL\DB018 底下檔案 某個日期之前資料夾 ex:D:\ETL\DB018\20180313 151,152
	 * D:\ETL\DB600\DOWNLOAD
	 * D:\ETL\DB600\Migration
	 * 
	 * @param deleteBefDate
	 * @throws Exception
	 */
	private static void dropETL(Date deleteBefDate) throws Exception {
		for (String central_no : central_nos) {

			File folder = new File(ETL_Tool_Folder.ETLDownLoadFilePath + central_no);
			ETL_Tool_Folder.dropSubfolderByCondition(folder, 0, 8, deleteBefDate);
			folder = new File(ETL_Tool_Folder.ETLDownLoadFilePath + central_no + "/DOWNLOAD");

			if (folder.exists()) {
				ETL_Tool_Folder.dropSubDocumentByCondition(folder, 10, 18, deleteBefDate, "txt");
				ETL_Tool_Folder.dropSubDocumentByCondition(folder, 8, 16, deleteBefDate, "txt");
				ETL_Tool_Folder.dropSubDocumentByCondition(folder, 13, 21, deleteBefDate, "txt");

				ETL_Tool_Folder.dropSubDocumentByCondition(folder, 8, 16, deleteBefDate, "zip");
				ETL_Tool_Folder.dropSubDocumentByCondition(folder, 11, 19, deleteBefDate, "zip");
				
			}
			
			folder = new File(ETL_Tool_Folder.ETLDownLoadFilePath + central_no + "/Migration");
			if (folder.exists())
				ETL_Tool_Folder.dropSubfolderByCondition(folder, 0, 8, deleteBefDate);
		}
	}

	/**
	 * 151.152
	 * @throws IOException
	 */
	private static void dropETLDM() throws IOException {
		File folder = new File(ETL_Tool_Folder.DMFilePath);
		List<File> list = listFilesForFolder(folder, ETL_Tool_Folder.Type.OnlyDocument);
		ETL_Tool_Folder.dropFileByList(list);
	}

	/**
	 * 133
	 * @param deleteBefDate
	 * @throws Exception
	 */
	private static void dropGAMLDM(Date deleteBefDate) throws Exception {
		File folder = new File(ETL_Tool_Folder.DMFilePath);
		// 清除該目錄符合條件的子檔案 txt
		ETL_Tool_Folder.dropSubDocumentByCondition(folder, 19, 27, deleteBefDate, "Txt");
		ETL_Tool_Folder.dropSubDocumentByCondition(folder, 21, 29, deleteBefDate, "Txt");
		ETL_Tool_Folder.dropSubDocumentByCondition(folder, 11, 19, deleteBefDate, "zip");
	}

	/**
	 * 刪除該目錄底下所有符合日期條件的檔案
	 * 
	 * @param folder
	 *            目錄
	 * @param dateStartIndex
	 *            日期的起始點
	 * @param dateEndIndex
	 *            日期的結束點
	 * @param deleteBefDate
	 *            日期基準點在此日期之前的都刪除
	 * @throws Exception
	 */
	public static void dropSubDocumentByCondition(File folder, int dateStartIndex, int dateEndIndex, Date deleteBefDate, String extension) throws Exception {
		File[] files = folder.listFiles();
		List<File> list = new ArrayList<File>();

		for (File file : files) {
			if (file.getName().length() < dateEndIndex || dateStartIndex > dateEndIndex || file.isDirectory() || !checkExtension(file, extension)) {
				continue;
			}
			String dateStr = file.getName().substring(dateStartIndex, dateEndIndex);
			Date fileDate = ETL_Tool_StringX.toUtilDate(dateStr);
			if (fileDate != null && deleteBefDate.after(fileDate)) {
				list.add(file);
			}

		}

		dropFileByList(list);
	}

	/**
	 * 刪除該目錄底下所有符合日期條件的文件
	 * 
	 * @param folder
	 *            目錄
	 * @param dateStartIndex
	 *            日期的起始點
	 * @param dateEndIndex
	 *            日期的結束點
	 * @param deleteBefDate
	 *            日期基準點在此日期之前的都刪除
	 * @throws Exception
	 */
	public static void dropSubfolderByCondition(File folder, int dateStartIndex, int dateEndIndex, Date deleteBefDate) throws Exception {
		File[] files = folder.listFiles();
		List<File> list = new ArrayList<File>();
		List<File> allFile = new ArrayList<File>();

		for (File file : files) {
			if (file.getName().length() < dateEndIndex || dateStartIndex > dateEndIndex || (!file.isDirectory())) {
				continue;
			}

			String dateStr = file.getName().substring(dateStartIndex, dateEndIndex);
			Date fileDate = ETL_Tool_StringX.toUtilDate(dateStr);

			if (fileDate != null && deleteBefDate.after(fileDate)) {
				list.add(file);
			}
		}

		for (File file : list) {
			allFile.addAll(listFilesForFolder(file, ETL_Tool_Folder.Type.ALLFile));
		}

		ETL_Tool_Folder.dropFileByList(allFile);
	}

	private static boolean checkExtension(File file, String extension) {
		int startIndex = file.getName().lastIndexOf(46) + 1;
		int endIndex = file.getName().length();
		return extension.toUpperCase().equals(file.getName().substring(startIndex, endIndex).toUpperCase());
	}
	
	public static boolean clearETLFile() {
		boolean isSuccess = false;
		
		Calendar cal = Calendar.getInstance(); // 今天時間
		cal.add(Calendar.DATE, KEEP_DAY * -1); // KEEP_DAY天前
		Date deleteBefDate = cal.getTime();
		
		try {
			dropETLDM();
			dropETL(deleteBefDate);
			isSuccess = true;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isSuccess = false;
		}
		return isSuccess;
	}
	
	public static boolean clearGAMLFile() {
		boolean isSuccess = false;

		Calendar cal = Calendar.getInstance(); // 今天時間
		cal.add(Calendar.DATE, KEEP_DAY * -1); // KEEP_DAY天前
		Date deleteBefDate = cal.getTime();
		
		try {
			
			dropDBArchlog();
			dropLoadBack();
			dropGAMLDownLoad(deleteBefDate);
			dropGAMLDM(deleteBefDate);
			isSuccess = true;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isSuccess = false;
		}

		return isSuccess;
	}

	public static void main(String[] args) throws Exception {

		// //清除該目錄底下的Document 不刪資料夾
		// File folder2 = new File("C:/ETL/LoadBack");
		// List<File> list = listFilesForFolder(folder2, ETL_Tool_Folder.Type.OnlyDocument);
		// ETL_Tool_Folder.dropFileByList(list);
		//
		// // 清除該目錄下所有資料
		// File folder1 = new File("D:\\company\\pershing\\agribank\\001 - 複製");
		// List<File> files = ETL_Tool_Folder.listFilesForFolder(folder1,
		// Type.ALLFileKeepFolder);
		// ETL_Tool_Folder.dropFileByList(files);
		//
		// // 清除該目錄符合條件的子資料夾
		// File folder = new File("D:\\company\\pershing\\agribank\\001");
		// ETL_Tool_Folder.dropSubfolderByCondition(folder, 0, 8,
		// ETL_Tool_StringX.toUtilDate("20180905"));
		//
		// // 清除該目錄符合條件的子檔案 txt
		// ETL_Tool_Folder.dropSubDocumentByCondition(folder, 10, 18,
		// ETL_Tool_StringX.toUtilDate("20180915"), "Txt");
		//
		// // 清除該目錄符合條件的子檔案 zip
		// ETL_Tool_Folder.dropSubDocumentByCondition(folder, 8, 16,
		// ETL_Tool_StringX.toUtilDate("20180921"), "zIp");
		//
		// //清除133上的各單位 DOWNLOAD 資料 ex:C:\ETL\DB018\DOWNLOAD
		// dropGAMLDownLoad(ETL_Tool_StringX.toUtilDate("20180713"));
		//dropGAMLDM(ETL_Tool_StringX.toUtilDate("20180911"));
		
		// 清理133的所有該清資料
		//clearGAMLFile();
		
		// 清理151的所有該清資料
		boolean isSuccess = clearETLFile();

		 System.out.println("執行結果 :" +isSuccess);

		 System.out.println("結束");
		



	}
}
