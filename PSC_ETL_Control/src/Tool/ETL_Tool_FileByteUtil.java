package Tool;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ibm.db2.jcc.am.l;

import Bean.ETL_Bean_ErrorLog_Data;
import DB.ETL_P_ErrorLog_Writer;
import Profile.ETL_Profile;

public class ETL_Tool_FileByteUtil {
	private FileInputStream fileInputStream = null;

	private List<byte[]> lineList = null;
	private int buffer_size = 0;
	private ETL_Tool_JBReader bufferedReader = null;

	public ETL_Tool_FileByteUtil() {
	}

	public ETL_Tool_FileByteUtil(String path, Class<?> clazz) throws FileNotFoundException {
		this.fileInputStream = new FileInputStream(path);
		this.bufferedReader = new ETL_Tool_JBReader(fileInputStream);

		String theme = clazz.getSimpleName();

		switch (theme) {
			case "ETL_E_PARTY":
				this.buffer_size = ETL_Profile.ETL_E_PARTY;
				break;
			case "ETL_E_PARTY_PARTY_REL":
				this.buffer_size = ETL_Profile.ETL_E_PARTY_PARTY_REL;
				break;
			case "ETL_E_PARTY_PHONE":
				this.buffer_size = ETL_Profile.ETL_E_PARTY_PHONE;
				break;
			case "ETL_E_PARTY_ADDRESS":
				this.buffer_size = ETL_Profile.ETL_E_PARTY_ADDRESS;
				break;
			case "ETL_E_ACCOUNT":
				this.buffer_size = ETL_Profile.ETL_E_ACCOUNT;
				break;
			case "ETL_E_TRANSACTION":
				this.buffer_size = ETL_Profile.ETL_E_TRANSACTION;
				break;
			case "ETL_E_TRANSACTION_OLD":
				this.buffer_size = ETL_Profile.ETL_E_TRANSACTION_OLD;
				break;
			case "ETL_E_LOAN_DETAIL":
				this.buffer_size = ETL_Profile.ETL_E_LOAN_DETAIL;
				break;
			case "ETL_E_LOAN":
				this.buffer_size = ETL_Profile.ETL_E_LOAN;
				break;
			case "ETL_E_COLLATERAL":
				this.buffer_size = ETL_Profile.ETL_E_COLLATERAL;
				break;
			case "ETL_E_GUARANTOR":
				this.buffer_size = ETL_Profile.ETL_E_GUARANTOR;
				break;
			case "ETL_E_FX_RATE":
				this.buffer_size = ETL_Profile.ETL_E_FX_RATE;
				break;
			case "ETL_E_SERVICE":
				this.buffer_size = ETL_Profile.ETL_E_SERVICE;
				break;
			case "ETL_E_TRANSFER":
				this.buffer_size = ETL_Profile.ETL_E_TRANSFER;
				break;
			case "ETL_E_FCX":
				this.buffer_size = ETL_Profile.ETL_E_FCX;
				break;
			case "ETL_E_CALENDAR":
				this.buffer_size = ETL_Profile.ETL_E_CALENDAR;
				break;
			case "ETL_E_AGENT": 
				this.buffer_size = ETL_Profile.ETL_E_AGENT; 
				break; 
			case "ETL_E_SCUSTBOX": 
				this.buffer_size = ETL_Profile.ETL_E_SCUSTBOX;
				break; 
			case "ETL_E_SCUSTBOXOPEN": 
				this.buffer_size = ETL_Profile.ETL_E_SCUSTBOXOPEN; 
				break; 
			case "ETL_E_SPARTY": 
				this.buffer_size = ETL_Profile.ETL_E_SPARTY; 
				break;
		}
	}

	public static byte[] toByteArrayUseMappedByte(String filename) throws IOException {

		FileChannel fc = null;
		try {
			fc = new RandomAccessFile(filename, "r").getChannel();
			MappedByteBuffer byteBuffer = fc.map(MapMode.READ_ONLY, 0, fc.size()).load();
			byte[] result = new byte[(int) fc.size()];
			if (byteBuffer.remaining() > 0) {
				byteBuffer.get(result, 0, byteBuffer.remaining());
			}
			return result;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} finally {
			try {
				fc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public List<byte[]> getFilesBytes() throws IOException {

		this.lineList = new ArrayList<byte[]>();

		for (int i = 0; i < ETL_Profile.ETL_E_Stage; i++) {
			byte[] line = bufferedReader.readLineInBinary();

			if (line != null)
				lineList.add(line);

		}
		return lineList;
	}

	public List<byte[]> getFilesByteForOneRow() throws IOException {

		this.lineList = new ArrayList<byte[]>();

		byte[] line = bufferedReader.readLineInBinary();
		if (line != null)
			lineList.add(line);

		return lineList;
	}

	public int isFileOK(ETL_Tool_ParseFileName pfn, String upload_no, String path) throws Exception {

		// ETL_Error Log寫入輔助工具
		ETL_P_ErrorLog_Writer errWriter = new ETL_P_ErrorLog_Writer();

		// 1:true 2: false 如格式都正確則是資料總筆數
		int isFileOK = 0;
		boolean isInsert = false;
//		byte[]bytes =new byte[99999999];
		byte[]bytes =new byte[50000000];
		
		FileInputStream fileInputStream = new FileInputStream(path);
		ETL_Tool_JBReader bufferedReader = new ETL_Tool_JBReader(fileInputStream, buffer_size);

		byte[] line = null;
		byte head = (byte) 49;
		byte body = (byte) 50;
		byte foot = (byte) 51;
		
		int index = 0;
		while ((line = bufferedReader.readLineInBinary()) != null) {
			bytes[index] = line[0];
			index++;
		}

		if (bytes.length < 2) {
			return isFileOK;
		}

		for (int i = 0; i < index; i++) {
			byte now = bytes[i];

			if (i == 0) {
				isFileOK = (head == now) ? 1 : 0;
				// break;
			} else if (i != (index - 1)) {
				isFileOK = (body == now) ? 1 : 0;
			} else {
				isFileOK = (foot == now) ? 1 : 0;
			}

			if (isFileOK == 0) {
				isInsert = true;
				
				// 寫入Error Log
				errWriter.addErrLog(new ETL_Bean_ErrorLog_Data(pfn, upload_no, "E", String.valueOf(i + 1), "區別碼",
						"解析檔案出現嚴重錯誤-區別碼錯誤"));
//				System.out.println("第" + i + "筆");
				errWriter.insert_Error_Log();
			}

		}

		return (isInsert)? 0 : index;
	}

	public int isFileOK(String path) throws IOException {

		int isFileOK = 0;
		List<Byte> list = new ArrayList<Byte>();

		FileInputStream fileInputStream = new FileInputStream(path);
		ETL_Tool_JBReader bufferedReader = new ETL_Tool_JBReader(fileInputStream, buffer_size);

		byte[] line = null;
		byte head = (byte) 49;
		byte body = (byte) 50;
		byte foot = (byte) 51;

		while ((line = bufferedReader.readLineInBinary()) != null) {
			list.add(line[0]);
		}

		if (list.size() < 2) {
			return isFileOK;
		}

		for (int i = 0; i < list.size(); i++) {
			byte now = list.get(i);
			if (i != 0 && isFileOK == 0)
				break;
			if (i == 0) {
				isFileOK = head == now ? 1 : 0;
				if (isFileOK == 0)
					break;
			} else if (i != (list.size() - 1)) {
				isFileOK = body == now ? 1 : 0;
			} else {
				isFileOK = foot == now ? 1 : 0;
			}

		}
		return isFileOK == 1 ? list.size() : 0;
	}

	private static byte[][] split_bytes(byte[] bytes, int copies) {

		double split_length = Double.parseDouble(copies + "");

		int array_length = (int) Math.ceil(bytes.length / split_length);
		// int array_length = 231939126;
		System.out.println("array_length:" + array_length);
		byte[][] result = new byte[array_length][];

		int from, to;

		for (int i = 0; i < array_length; i++) {

			from = (int) (i * split_length);
			to = (int) (from + split_length);

			if (to > bytes.length)
				to = bytes.length;

			result[i] = Arrays.copyOfRange(bytes, from, to);
		}

		return result;
	}

	private static byte[] hexStrToByteArray(String str) {
		if (str == null) {
			return null;
		}
		if (str.length() == 0) {
			return new byte[0];
		}
		byte[] byteArray = new byte[str.length() / 2];
		for (int i = 0; i < byteArray.length; i++) {
			String subStr = str.substring(2 * i, 2 * i + 2);
			byteArray[i] = ((byte) Integer.parseInt(subStr, 16));
		}
		return byteArray;
	}

	public static void main(String[] args) throws Exception {
		byte head = (byte) 49;

		byte[] line = { head };

		System.out.println(new String(line, "big5"));
		System.out.println(new String(line));
	}

	public void main2(String[] args) throws Exception {

		// String path =
		// "C:\\Users\\Ian\\Desktop\\018\\018_CALENDAR_20180116.TXT";
		//
		// long time1, time2;
		// time1 = System.currentTimeMillis();
		// // getFilesBytes(path);
		// time2 = System.currentTimeMillis();
		// System.out.println("花了：" + (time2 - time1) + "豪秒");
	}
}