package Control;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import Bean.ETL_Bean_MailFilter;
import Tool.ETL_Tool_Mail;

public class ETL_C_MailSender {
	
	// 寄發信件批次
	public void execute() {
		
		List<ETL_Bean_MailFilter> mailList = ETL_Tool_Mail.getSendMails();
		
		if (mailList == null || mailList.size() == 0) {
			System.out.println("無需要寄發信件"); // for test
			return;
		}
		
		System.out.println("####ETL_C_MailSender 寄發通知信  Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		System.out.println("信件數：" + mailList.size());
		
		for (int i = 0; i < mailList.size(); i++) {
			try {
				ETL_Bean_MailFilter oneMail = mailList.get(i);
				if (ETL_Tool_Mail.sendMail(
						oneMail.getReceivers(), oneMail.getCcReceivers(), oneMail.getBccReceivers(),
						oneMail.getSubject(), oneMail.getContent())) {
					// 發信成功, 更新信件狀態
					ETL_Tool_Mail.updateAML_Mail(oneMail.getPkey(), 2);
					
				} else {
					// 發信失敗, 更新信件狀態
					ETL_Tool_Mail.updateAML_Mail(oneMail.getPkey(), 4);
				}
				
			} catch (Exception ex) {				
				ex.printStackTrace();
				// 發信失敗, 更新信件狀態
				ETL_Tool_Mail.updateAML_Mail(mailList.get(i).getPkey(), 4);
			}
		}
		
		System.out.println("####ETL_C_MailSender 寄發通知信  End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
	}

}
