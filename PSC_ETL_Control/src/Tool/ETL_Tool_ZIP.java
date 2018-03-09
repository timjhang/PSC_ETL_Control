package Tool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.io.ZipInputStream;
import net.lingala.zip4j.model.FileHeader;
import net.lingala.zip4j.unzip.UnzipUtil;

public class ETL_Tool_ZIP {
	
	public static void main(String[] args) throws IOException {
		
		String zipPath = "C:\\test2\\ZIP\\123.zip";
		String descDir = "C:\\test2\\ZIP";
		
		ETL_Tool_ZIP.extractZipFiles(zipPath, descDir, "5566");
		
	}
	
	private static final int BUFF_SIZE = 4096;

    public static boolean extractZipFiles(String zipPath, String descDir, String password) {
        ZipInputStream is = null;
        OutputStream os = null;
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
                    
                    // 使用完畢關閉變數
                    closeFileHandlers(is, os);
                    
                    UnzipUtil.applyFileAttributes(fileHeader, outFile);
                    System.out.println("Done extracting: " + fileHeader.getFileName());
                } else {
                    System.err.println("fileheader is null. Shouldn't be here");
                }
            }
            
            return true;
        } catch (ZipException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                closeFileHandlers(is, os);
            } catch (IOException e) {
//                e.printStackTrace();
            }
        }
        
        return false;
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
