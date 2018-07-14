package Tool;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.ibm.db2.jcc.DB2Types;

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
	public static List<String> getRoleEmails(String central_no, String roles) {
		List<String> resultList = new ArrayList<String>();

		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;

		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.getRoleEmails(?,?,?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
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

			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		return resultList;
	}

	public static void main(String args[]) throws AddressException {
		
		System.out.println("測試  Start");
		
		List<String> sendToUsers = ETL_Tool_Mail.getRoleEmails("600", "UserInfo");
		
		for (int i = 0; i < sendToUsers.size(); i++) {
			System.out.println(sendToUsers.get(i));
		}
		
//		List<String> ccUsers =  ETL_Tool_Mail.getEmails(null, "Administrator");

		// sendToUsers.add("timjhang@pershing.com.tw");
		// ccUsers.add("ciss00217@gmail.com");
		// ccUsers.add("ianchien@pershing.com.tw");

		//ETL_Tool_Mail.sendMail(sendToUsers, ccUsers, "測試", "內容");
		
		System.out.println("測試  End");
	}

}
