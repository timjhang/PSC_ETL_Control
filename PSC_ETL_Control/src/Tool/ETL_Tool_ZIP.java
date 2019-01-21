package Tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.ZipInputStream;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.unzip.UnzipUtil;
import net.lingala.zip4j.util.Zip4jConstants;

public class ETL_Tool_ZIP {
	
	public static void main(String[] args) throws IOException {
		
		String zipPath = "C:\\Users\\10404003\\Desktop\\農金\\2018\\181107\\test2\\AML_TR_600_20181005001.zip";
		String descDir = "C:\\Users\\10404003\\Desktop\\農金\\2018\\181107\\test2\\AML_TR_600_20181005001\\";
		
		ETL_Tool_ZIP.extractZipFiles(zipPath, descDir, "d1fa8edf36");
		
//		//檔案進行更名
//		if (renameMigrationFiles(descDir)) {
//			System.out.println(descDir + "  內檔案去  \"TR_\"  結束!");
//		} else {
//			System.out.println(descDir + "  內檔案去  \"TR_\"  失敗!");
//		}
		
	}
	
	private static final int BUFF_SIZE = 4096;

    public static boolean extractZipFiles(String zipPath, String descDir, String password) {
        ZipInputStream is = null;
        OutputStream os = null;
        
        List<ZipInputStream> isList = new ArrayList<ZipInputStream>();
        List<OutputStream> osList = new ArrayList<OutputStream>();
        
        try {
            ZipFile zipFile = new ZipFile(zipPath);
            
            // 若檔案有加密碼, 則設定解壓縮密碼
            if (zipFile.isEncrypted()) {
                zipFile.setPassword(password);
            }
            
            // 取得所有ZipFile檔案內的檔名
            List fileHeaderList = zipFile.getFileHeaders();
            
            // 處理所有的解壓縮黨
            for (int i = 0; i < fileHeaderList.size(); i++) {
            	
                FileHeader fileHeader = (FileHeader) fileHeaderList.get(i);
                if (fileHeader != null) {
                    
                	// 解壓縮檔案位置(含檔名)
                    String outFilePath = descDir
                        + System.getProperty("file.separator")
                        + fileHeader.getFileName();
                    File outFile = new File(outFilePath);
                    
                    // 若檔案是一個目錄, 建立一個資料夾目錄
                    if (fileHeader.isDirectory()) {
                    	
                        outFile.mkdirs();
                        continue;
                    }
                    
                    // 輸出路徑母目錄, 若不存在則建立一個資料夾目錄
                    File parentDir = outFile.getParentFile();
                    if (!parentDir.exists()) {
                        parentDir.mkdirs();
                    }
                    
                    // 取得解壓縮檔案InputStream
                    is = zipFile.getInputStream(fileHeader);
                    
                    // 初始化產生檔案PutputStream
                    os = new FileOutputStream(outFile);
                    int readLen = -1;
                    byte[] buff = new byte[BUFF_SIZE];
                    
                    // 轉換寫入解壓縮檔
                    while ((readLen = is.read(buff)) != -1) {
                        os.write(buff, 0, readLen);
                    }
                    
//                    // 使用完畢關閉變數
//                    closeFileHandlers(is, os);
                    // 加入is, os List 待處理後關閉連線
                    isList.add(is);
                    osList.add(os);
                    
                    UnzipUtil.applyFileAttributes(fileHeader, outFile);
                    System.out.println("Done extracting: " + fileHeader.getFileName());
                } else {
                    System.err.println("fileheader is null. Shouldn't be here");
                }
            }
            
            return true;
        } catch (ZipException ex) {
        	ex.printStackTrace();
            StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:" + sw.toString());
        } catch (FileNotFoundException ex) {
        	ex.printStackTrace();
            StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:" + sw.toString());
        } catch (IOException ex) {
        	ex.printStackTrace();
            StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:" + sw.toString());
        } catch (Exception ex) {
        	ex.printStackTrace();
            StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:" + sw.toString());
        } finally {
        	// 使用完畢關閉變數
        	for (int i = 0; i < isList.size(); i++) {
        		try {
        			closeFileHandlers(isList.get(i), osList.get(i));
            	} catch (Exception ex) {
//              	ex.printStackTrace();
            	}
            }
        }
        
        return false;
    }
    
    /**
     * 
     * @param files	 為要壓縮的 file List
     * @param zipFilePath 壓縮後的zip絕對路徑
     * @param adPW	壓縮的密碼
     * @return	壓縮後的zip File
     * 
     */
	public static File toZip(ArrayList<File> files, String zipFilePath, String adPW) {
		ZipParameters parameters = new ZipParameters();
		ZipFile zipFile;
		try {
			zipFile = new ZipFile(zipFilePath);

			parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
			parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
			parameters.setEncryptionMethod(Zip4jConstants.ENC_METHOD_AES);
			parameters.setAesKeyStrength(Zip4jConstants.AES_STRENGTH_256);
			parameters.setEncryptFiles(true);
			parameters.setPassword(adPW);

			zipFile.addFiles(files, parameters);

			return zipFile.getFile();

		} catch (ZipException ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:" + sw.toString());
			return null;
		}

	}

    private static void closeFileHandlers(ZipInputStream is, OutputStream os)
            throws IOException {
        // Close output stream
        if (os != null) {
            os.close();
            os = null;
        }
        // Closing inputstream also checks for CRC of the the just extracted
        // file. If CRC check has to be skipped (for ex: to cancel the unzip
        // operation, etc) use method is.close(boolean skipCRCCheck) and set the
        // flag, skipCRCCheck to false
        // NOTE: It is recommended to close outputStream first because Zip4j
        // throws an exception if CRC check fails
        if (is != null) {
            is.close();
            is = null;
        }
    }
	

//    public static void unZipFiles(String zipPath, String descDir) throws IOException {  
//        unZipFiles(new File(zipPath), descDir); 
//    }  
// 
//    @SuppressWarnings("rawtypes")
//    public static void unZipFiles(File zipFile, String descDir) throws IOException {  
//        File pathFile = new File(descDir);  
//        if (!pathFile.exists()) {  
//        	pathFile.mkdirs();  
//        }  
//        ZipFile zip = new ZipFile(zipFile);
//        for (Enumeration entries = zip.entries(); entries.hasMoreElements();) {  
//            ZipEntry entry = (ZipEntry)entries.nextElement();  
//            String zipEntryName = entry.getName();  
//            InputStream in = zip.getInputStream(entry);  
//            String outPath = descDir + zipEntryName;
//
//            OutputStream out = new FileOutputStream(outPath);  
//            byte[] buf1 = new byte[1024];  
//            int len;  
//            while ((len = in.read(buf1)) > 0) {  
//                out.write(buf1, 0, len);  
//            }
//            in.close();  
//            out.close();  
//		}  
//        System.out.println("******************解壓縮完畢********************");  
//    }

}
