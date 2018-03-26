package Tool;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import Profile.ETL_Profile;

public class ETL_Tool_StringQueue {
	// 截字輔助工具截byte(預設轉BIG5)
	
	// 轉換編碼格式
	private static final String format = "BIG5";
//	private static final String format = "UTF-8";
	
	// 字串總長度
//	private int totalLength = 0;
	// 字串
	private String targetString;
	// 已pop長度
//	private int popOutLength = 0;
	
	// 字串總長度(byte)
	private int totalByteLength = 0;
	// 字串(byte)
	private byte[] targetStringBytes;
	// 已pop Bytes長度
	private int popOutBytesLength = 0;
	
	// 載入檔案以0D0A分割而成bytes Array List
	private List<byte[]> bytesList;
	// bytesList index(標定使用的targetStringBytes)
	private int bytesListIndex = 0;
	// byteList  sizw
	private int byteListSize = 0;
	
	// 難字轉換工具
	ETL_Tool_Big5_To_UTF8 wordsXTool;
	// 難字轉換map
	Map<String, Map<String, String>> difficultWordMaps;
	
	// class生成時, 按報送單位, 取得所有單位難字表
	public ETL_Tool_StringQueue(String central_No) {
		wordsXTool = new ETL_Tool_Big5_To_UTF8(ETL_Profile.DifficultWords_Lists_Path);
		difficultWordMaps = wordsXTool.getDifficultWordMaps(central_No);
		if (difficultWordMaps == null) {
			System.out.println("#########  報送單位:" + central_No + " 無法取得難字表！！  #########");
//			throw new Exception("報送單位:" + central_No + " 無法取得難字表！！"); // for test
		}
	}
	

	public String getTargetString() {
		return targetString;
	}
	
	// 取得字串Byte長度
	public int getTotalByteLength() {
		return totalByteLength;
	}
	
	public List<byte[]> getBytesList() {
		return bytesList;
	}

	public void setBytesList(List<byte[]> bytesList) {
		// 計入list大小
		this.byteListSize = (bytesList!=null)?bytesList.size():0;
		// 裝入list
		this.bytesList = bytesList;
	}
	
	public int getBytesListIndex() {
		return bytesListIndex;
	}


	public void setBytesListIndex(int bytesListIndex) {
		this.bytesListIndex = bytesListIndex;
	}


	public int getByteListSize() {
		return byteListSize;
	}


	// 注入字串, 並設定相關參數  test temp  // 這個方法考慮不使用
	// 重新set後, 完全更新Queue
	public void setTargetString(String targetString) throws UnsupportedEncodingException {
//		this.totalLength = targetString.length();
//		this.targetString = targetString;
//		this.popOutLength = 0;
		
		this.totalByteLength = targetString.getBytes(format).length;
		this.targetStringBytes = targetString.getBytes(format);
		this.popOutBytesLength = 0;
	}
	
	public int setTargetString() {	// 重新set後, 完全更新Queue, 回傳已使用array數
//System.out.println("tool:"+bytesListIndex);
		this.targetStringBytes = bytesList.get(bytesListIndex);
		this.totalByteLength = this.targetStringBytes.length;
		this.popOutBytesLength = 0;
		
		bytesListIndex++;
		return bytesListIndex;
	}
	
	// pop出擷取字串(字串長度切)
//	public String popString(int popLength) throws Exception {
//		if (popLength < 0) {
//			throw new Exception("ETL_Tool_TokenString - popString - 輸入參數須為正！");
//		}
//		
//		if (popOutLength >= totalLength) {
//			throw new Exception("ETL_Tool_TokenString - popString - 字串已pop完畢!");
//		}
//		
//		if (popOutLength + popLength > totalLength) {
//			throw new Exception("ETL_Tool_TokenString - popString - 超出pop範圍!");
//		}
//		
//		// pop起始位置
//		int tokenStartIndex = popOutLength;
//		// 累加已pop次數
//		popOutLength = popOutLength + popLength;
//		
//		// 回傳擷取String
//		return targetString.substring(tokenStartIndex, popOutLength);
//		
//	}
	
	// pop出擷取bytes(bytes長度切)
	public byte[] popBytes(int popLength) throws Exception {
		if (popLength < 0) {
			throw new Exception("ETL_Tool_TokenString - popBytes - 輸入參數須為正！");
		}
		
		if (popOutBytesLength >= totalByteLength) {
			throw new Exception("ETL_Tool_TokenString - popBytes - 字串已pop完畢!");
		}
		
		if (popOutBytesLength + popLength > totalByteLength) {
			throw new Exception("ETL_Tool_TokenString - popBytes - 超出pop範圍!");
		}
		
		// 結果array
		byte[] resultBytes = new byte[popLength];
		// pop起始位置
		int tokenStartIndex = popOutBytesLength;
		// 累加已pop次數
		popOutBytesLength = popOutBytesLength + popLength;
		
		// 複製所需長度array至resultBytes上
		System.arraycopy(targetStringBytes, tokenStartIndex, resultBytes, 0, popLength);
		
		// 回傳結果array
		return resultBytes;
	}
	
	// pop出擷取字串(bytes長度切)
	public String popBytesString(int popLength) throws Exception {
		if (popLength < 0) {
//			throw new Exception("ETL_Tool_TokenString - popBytes - 輸入參數須為正！");
			System.out.println("ETL_Tool_TokenString - popBytes - 輸入參數須為正！");
			return "";
		}
		
		if (popOutBytesLength >= totalByteLength) {
//			throw new Exception("ETL_Tool_TokenString - popBytes - 字串已pop完畢!");
			System.out.println("ETL_Tool_TokenString - popBytes - 字串已pop完畢!");
			return "";
		}
		
		if (popOutBytesLength + popLength > totalByteLength) {
//			throw new Exception("ETL_Tool_TokenString - popBytes - 超出pop範圍!");
			System.out.println("ETL_Tool_TokenString - popBytes - 超出pop範圍!");
			// 截出最後的內容
			popLength = totalByteLength - popOutBytesLength;
		}
		
		// 結果array
		byte[] resultBytes = new byte[popLength];
		// pop起始位置
		int tokenStartIndex = popOutBytesLength;
		// 累加已pop次數
		popOutBytesLength = popOutBytesLength + popLength;
		
		// 複製所需長度array至resultBytes上
		System.arraycopy(targetStringBytes, tokenStartIndex, resultBytes, 0, popLength);
		
		// 回傳結果array
		return new String(resultBytes, format);
	}
	
	// pop出擷取字串(bytes長度切), 難字經過轉換
	public String popBytesDiffString(int popLength) throws Exception {
		if (popLength < 0) {
			System.out.println("ETL_Tool_TokenString - popBytes - 輸入參數須為正！");
			return "";
		}
		
		if (popOutBytesLength >= totalByteLength) {
			System.out.println("ETL_Tool_TokenString - popBytes - 字串已pop完畢!");
			return "";
		}
		
		if (popOutBytesLength + popLength > totalByteLength) {
			System.out.println("ETL_Tool_TokenString - popBytes - 超出pop範圍!");
			// 截出最後的內容
			popLength = totalByteLength - popOutBytesLength;
		}
		
		// 結果array
		byte[] resultBytes = new byte[popLength];
		// pop起始位置
		int tokenStartIndex = popOutBytesLength;
		// 累加已pop次數
		popOutBytesLength = popOutBytesLength + popLength;
		
		// 複製所需長度array至resultBytes上
		System.arraycopy(targetStringBytes, tokenStartIndex, resultBytes, 0, popLength);
		
		if (difficultWordMaps != null) {
			return wordsXTool.format(resultBytes, difficultWordMaps);
		} else {
			return new String(resultBytes, format);
		}
	}
	
	// test
	public static void main(String[] argv) {
		try {
//			ETL_Tool_StringQueue one = new ETL_Tool_StringQueue();
			String temp = "臣亮言：先帝創業未半，而中道崩殂。今天下三分，益";
//			one.setTargetString(temp);
//			System.out.println(one.popString(1));
//			System.out.println(one.popString(2));
//			System.out.println(one.popString(10));
//			System.out.println(one.popString(temp.length() - (1 + 2 + 10)));
			
//			System.out.println(new String(one.popBytes(3), one.format));
//			System.out.println(one.popBytesString(3));
			
			temp = "123牽著手456抬起頭";
//			one.setTargetString(temp);
//			System.out.println(one.getTotalByteLength());
//			System.out.println(one.popBytesString(9));
//			System.out.println(one.getTotalByteLength());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
