package Tool;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.ibm.db2.jcc.DB2Types;

import Bean.ETL_Bean_MailFilter;
import Bean.ETL_Bean_MailReceiveUnit;
import DB.ConnectionHelper;
import Profile.ETL_Profile;

public class ETL_Tool_Mail {

	// 寄發mail
	public static boolean sendMail(List<String> sendToUsers, List<String> ccUsers, List<String> bccUsers, String subject, String content) {
		
		Properties props = new Properties();
		props.put("mail.smtp.host", ETL_Profile.EMAIL_SERVER_HOST);
		props.put("mail.smtp.port", ETL_Profile.EMAIL_SERVER_PORT);

		Session session = Session.getDefaultInstance(props);
		Message message = new MimeMessage(session);

		try {

			message.setFrom(new InternetAddress(ETL_Profile.EMAIL_SERVER_ADDRESS));
			if (sendToUsers != null && sendToUsers.size() > 0) {
				message.setRecipients(Message.RecipientType.TO, getSendTOAddress(sendToUsers));
			}
			
			if (ccUsers != null && ccUsers.size() > 0) {
				message.setRecipients(Message.RecipientType.CC, getCCAddress(ccUsers));
			}
			
			if (bccUsers != null && bccUsers.size() > 0) {
				message.setRecipients(Message.RecipientType.BCC, getCCAddress(bccUsers));
			}

			message.setSubject(subject);

			message.setText("Hi\n\n" + content + "\n\nBest Regards\n(此為系統發送的通知信請勿直接回覆)");
			
			Transport.send(message);

			System.out.println("寄送email結束.");
			
			return true;

		} catch (MessagingException ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
			return false;
		}

	}

	// 組成寄信地址 Array
	private static Address[] getSendTOAddress(List<String> sendToUsers) throws AddressException {
		String sendTO = "";

		for (String user : sendToUsers) {
			sendTO = sendTO + user + ",";
		}

		sendTO = sendTO.substring(0, sendTO.length() - 1);

		return InternetAddress.parse(sendTO);
	}

	// 組成CC地址Array
	private static Address[] getCCAddress(List<String> ccUsers) throws AddressException {
		String CC = "";

		for (String user : ccUsers) {
			CC = CC + user + ",";
		}

		CC = CC.substring(0, CC.length() - 1);

		return InternetAddress.parse(CC);
	}
	
	// 取得角色(Role) Email
	private static List<String> getRoleEmails(String central_no, String roles) {
		List<String> resultList = new ArrayList<String>();

		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;

		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.getRoleEmails(?,?,?,?,?)}";
//			String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.getRoleEmails(?,?,?,?,?)}"; // for test

			con = ConnectionHelper.getDB2Connection();
//			con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, central_no);
			cstmt.setString(3, roles);
			cstmt.registerOutParameter(4, DB2Types.CURSOR);
			cstmt.registerOutParameter(5, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return resultList;
			}

			// (java.sql.ResultSet)
			rs = (java.sql.ResultSet) cstmt.getObject(4);

			if (rs == null) {
				return resultList;
			}

			while (rs.next()) {
				String email = rs.getString(1);
				if (email != null) {
					email = email.trim();
					resultList.add(email);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}

		}

		return resultList;
	}
	
	// 取得角色所有mail
	private static List<String> getRolesMailList(String central_no, String roles) {
		List<String> resuktList = new ArrayList<String>();
		
		if (roles == null || "".equals(roles.trim())) {
			return resuktList;
		}
		
		String[] role_Array = roles.split("\\;");
		
		List<String> roleList = new ArrayList<String>();
		for (int i = 0; i < role_Array.length; i++) {
			if (role_Array[i] != null && !"".equals(role_Array[i].trim())) {
				roleList.add(role_Array[i].trim());
			}
		}
		
		if (roleList.size() == 0) {
			return resuktList;
		}
		
		for (int i = 0; i < roleList.size(); i++) {
			List<String> roleMails = getRoleEmails(central_no, roleList.get(i));
			
			if (roleMails != null) {
				for (int j = 0; j < roleMails.size(); j++) {
					resuktList.add(roleMails.get(j).trim());
				}
			}
		}
		
		return resuktList;
	}
	
	// 解析當中字串中E-mail
	private static List<String> parseMailList(String mailStr) {
		List<String> resuktList = new ArrayList<String>();
		
		if (mailStr == null || "".equals(mailStr.trim())) {
			return resuktList;
		}
		
		String[] mailArray = mailStr.split("\\;");
		
		for (int i = 0; i < mailArray.length; i++) {
			if (mailArray[i] != null && !"".equals(mailArray[i].trim())) {
				resuktList.add(mailArray[i].trim());
			}
		}
		
		return resuktList;
	}
	
	// 統整所有收件mail, 排除掉不收件的mail
	private static List<String> getNeedSendMails(
			List<String> roleMails, List<String> receiverMails, List<String> notReceiveMails) {
		
		// 過濾重複mail
		Map<String, String> filterMap = new HashMap<String, String>();
		
		// 加入角色mail
		if (roleMails != null && roleMails.size() != 0) {
			for (int i = 0; i < roleMails.size(); i++) {
				if (roleMails.get(i) != null && !filterMap.containsKey(roleMails.get(i).trim())) {
					filterMap.put(roleMails.get(i).trim(), null);
				}
			}
		}
		
		// 加入預設mail
		if (receiverMails != null && receiverMails.size() != 0) {
			for (int i = 0; i < receiverMails.size(); i++) {
				if (receiverMails.get(i) != null && !filterMap.containsKey(receiverMails.get(i).trim())) {
					filterMap.put(receiverMails.get(i).trim(), null);
				}
			}
		}
		
		// 排除不加入mail
		if (notReceiveMails != null && notReceiveMails.size() != 0) {
			for (int i = 0; i < notReceiveMails.size(); i++) {
				if (notReceiveMails.get(i) != null && filterMap.containsKey(notReceiveMails.get(i).trim())) {
					filterMap.remove(notReceiveMails.get(i).trim());
				}
			}
		}
		
		// 轉成結果List
		List<String> resultList = new ArrayList<String>();
		if (!filterMap.isEmpty()) {
			Set<Map.Entry<String,String>> entrySet = filterMap.entrySet();
			Iterator<Map.Entry<String,String>> it = entrySet.iterator();
			 
			while (it.hasNext()) {
				Map.Entry<String,String> me = it.next();
				resultList.add(me.getKey());
			}
		}
		
		return resultList;
	}
	
	// 取得單位mail
	private static List<String> getUnitMails(String mail_type, String type_value) {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		List<String> resultList = new ArrayList<String>();
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.getUnitMails(?,?,?,?,?,?,?)}";
//			String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.getUnitMails(?,?,?,?,?,?,?)}"; // for test

			con = ConnectionHelper.getDB2Connection();
//			con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, mail_type);
			cstmt.setString(3, type_value);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			cstmt.registerOutParameter(7, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(7);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return resultList;
			}

			String receive_role = cstmt.getString(4);
			String receive_mail = cstmt.getString(5);
			String not_receive_role = cstmt.getString(6);
			
			if (!"Center".equals(mail_type)) {
				type_value = null;
			}
			resultList = ETL_Tool_Mail.getNeedSendMails(
					getRolesMailList(type_value, receive_role), parseMailList(receive_mail), parseMailList(not_receive_role));

			return resultList;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}
		}
		
		return resultList;
	}
	
	// mail List 轉換為String
	private static String getMailString(List<String> mailList) {
		String result = "";
		
		if (mailList == null || mailList.size() == 0) {
			return "";
		}
		
		for (int i = 0; i < mailList.size(); i++) {
			if (mailList.get(i) != null) {
				result += mailList.get(i).trim();
			}
			
			if (i != (mailList.size()-1)) {
				result += ";";
			}
		}
		
		return result;
	}
	
	// 取得待寄信件
	public static List<ETL_Bean_MailFilter> getSendMails() {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		List<ETL_Bean_MailFilter> resultList = new ArrayList<ETL_Bean_MailFilter>();
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.getAML_Mails(?,?,?)}";
//			String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.getAML_Mails(?,?,?)}"; // for test

			con = ConnectionHelper.getDB2Connection();
//			con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, DB2Types.CURSOR);
			cstmt.registerOutParameter(3, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return resultList;
			}

			// (java.sql.ResultSet)
			rs = (java.sql.ResultSet) cstmt.getObject(2);

			if (rs == null) {
				return resultList;
			}

			while (rs.next()) {
				ETL_Bean_MailFilter one = new ETL_Bean_MailFilter();
				one.setPkey(rs.getLong(1));
				one.setReceivers(parseMailList(rs.getString(2)));
				one.setCcReceivers(parseMailList(rs.getString(3)));
				one.setBccReceivers(parseMailList(rs.getString(4)));
				one.setSubject(rs.getString(5));
				one.setContent(rs.getString(6));
				
				resultList.add(one);
			}

			return resultList;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}
		}
		
		return resultList;
	}
	
	// 更新信件狀態
	public static void updateAML_Mail(Long Pkey, int ProcessFlag) {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		List<ETL_Bean_MailFilter> resultList = new ArrayList<ETL_Bean_MailFilter>();
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.updateAML_Mail(?,?,?,?)}";
//			String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.updateAML_Mail(?,?,?,?)}"; // for test

			con = ConnectionHelper.getDB2Connection();
//			con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setLong(2, Pkey);
			cstmt.setInt(3, ProcessFlag);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}
		}
	}
	
	// 寫入AML寄件Table(一般ETL發信中心OP及相關人員用, 其他不建議使用)
	public static boolean writeAML_Mail(String receiveUnit, String ccUnit, String bccUnit, String subject, String Content) {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.insertAML_Mails(?,?,?,?,?,?,?)}";
//			String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.insertAML_Mails(?,?,?,?,?,?,?)}"; // for test

			// 系統收件者
			List<String> receiveList = new ArrayList<String>();
			if (receiveUnit != null) {
				if ("SYSAdmin".equals(receiveUnit)) {
					receiveList = getUnitMails("SYSAdmin", receiveUnit);
				} else {
					receiveList = getUnitMails("Center", receiveUnit);
				}
			}
			String receiveListArray = getMailString(receiveList);
			
			// cc收件者
			List<String> ccList = new ArrayList<String>();
			if (ccUnit != null) {
				if ("SYSAdmin".equals(ccUnit)) {
					ccList = getUnitMails("SYSAdmin", ccUnit);
				} else {
					ccList = getUnitMails("Center", ccUnit);
				}
			}
			String ccListArray = getMailString(ccList);
			
			// bcc收件者
			List<String> bccList = new ArrayList<String>();
			if (bccUnit != null) {
				if ("SYSAdmin".equals(bccUnit)) {
					bccList = getUnitMails("SYSAdmin", bccUnit);
				} else {
					bccList = getUnitMails("Center", bccUnit);
				}
			}
			String bccListArray = getMailString(bccList);
			
			con = ConnectionHelper.getDB2Connection();
//			con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, receiveListArray);
			cstmt.setString(3, ccListArray);
			cstmt.setString(4, bccListArray);
			cstmt.setString(5, subject);
			cstmt.setString(6, Content);
			cstmt.registerOutParameter(7, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(7);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return false;
			}

			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
			return false;
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}
		}
	}
	
	// 寫入AML寄件Table(物件傳遞版, 特別關係人設定用版本)
	public static boolean writeAML_Mail(
			ETL_Bean_MailReceiveUnit receiveUnit, ETL_Bean_MailReceiveUnit ccUnit, ETL_Bean_MailReceiveUnit bccUnit,
			String subject, String Content) {
		
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.insertAML_Mails(?,?,?,?,?,?,?)}";
//			String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.insertAML_Mails(?,?,?,?,?,?,?)}"; // for test

			// 系統收件者
			List<String> receiveList = new ArrayList<String>();
			if (receiveUnit != null) {
				receiveList = getUnitMails(receiveUnit.getMail_type(), receiveUnit.getType_value());
			}
			String receiveListArray = getMailString(receiveList);
			
			// cc收件者
			List<String> ccList = new ArrayList<String>();
			if (ccUnit != null) {
				ccList = getUnitMails(ccUnit.getMail_type(), ccUnit.getType_value());
			}
			String ccListArray = getMailString(ccList);
			
			// bcc收件者
			List<String> bccList = new ArrayList<String>();
			if (bccUnit != null) {
				bccList = getUnitMails(bccUnit.getMail_type(), bccUnit.getType_value());
			}
			String bccListArray = getMailString(bccList);
			
			con = ConnectionHelper.getDB2Connection();
//			con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, receiveListArray);
			cstmt.setString(3, ccListArray);
			cstmt.setString(4, bccListArray);
			cstmt.setString(5, subject);
			cstmt.setString(6, Content);
			cstmt.registerOutParameter(7, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(7);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return false;
			}

			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
			return false;
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}
		}
		
	}

	public static void main(String args[]) throws AddressException {
		
		System.out.println("測試  Start");
		
		
//		if (writeAML_Mail("018", "SYSAdmin", null, "測試信件3", "測試內容3")) {
//			System.out.println("執行成功");
//		} else {
//			System.out.println("執行失敗");
//		}
		
//		List<String> list = getUnitMails("Center", "600");
//		List<String> list = getUnitMails("Center", "018");
//		List<String> list = getUnitMails("SYSAdmin", "SYSAdmin");
//		
//		for (int i = 0; i < list.size(); i++) {
//			System.out.println(list.get(i));
//		}
		
//		List<String> sendToUsers = ETL_Tool_Mail.getRoleEmails("600", "UserInfo");
//		
//		for (int i = 0; i < sendToUsers.size(); i++) {
//			System.out.println(sendToUsers.get(i));
//		}
		
//		List<String> ccUsers =  ETL_Tool_Mail.getEmails(null, "Administrator");

		List<String> sendToUsers = new ArrayList<String>();
		sendToUsers.add("timjhang@pershing.com.tw");
		// ccUsers.add("ciss00217@gmail.com");
		// ccUsers.add("ianchien@pershing.com.tw");

//		ETL_Tool_Mail.sendMail(sendToUsers, ccUsers, "測試", "內容");
		ETL_Tool_Mail.sendMail(sendToUsers, null, null, "測試", "內容");
		
		///////
//		List<String> list1 = new ArrayList<String>();
//		List<String> list2 = new ArrayList<String>();
//		List<String> list3 = new ArrayList<String>();
//		
//		list1.add("Tim1");
//		list1.add("Tim2");
//		list1.add("Tim2");
//		list1.add("Tim3");
//		
//		list2.add("Tim2");
//		list2.add("Tim3");
//		list2.add("Tim4");
//		list2.add("Tim5");
//		list2.add("Tim6");
//		
//		list3.add("Tim5");
//		list3.add("Tim1");
//		
//		List<String> list4 = getNeedSendMails(list1, list2, list3);
//		for (int i = 0; i < list4.size(); i++) {
//			System.out.println(list4.get(i));
//		}
//		
//		System.out.println(getMailString(list4));
		///////
		
//		List<ETL_Bean_MailFilter> list = getSendMails();
//		
//		System.out.println(list.size());
//		for (int i = 0; i < list.size(); i++) {
//			System.out.println("i = " + i);
//			System.out.println(list.get(i).getPkey());
//			System.out.println(list.get(i).getSubject());
//			System.out.println(list.get(i).getContent());
//			
//			System.out.println(getMailString(list.get(i).getReceivers()));
//			System.out.println(getMailString(list.get(i).getCcReceivers()));
//			System.out.println(getMailString(list.get(i).getBccReceivers()));
//		}
		
//		updateAML_Mail(1L, 3);
		
		System.out.println("測試  End");
	}

}
