package Bean;

import java.util.List;

public class ETL_Bean_MailFilter {
	
	// 鍵值
	private Long Pkey;
	// 收件者
	private List<String> receivers;
	// CC群
	private List<String> ccReceivers;
	// BCC群
	private List<String> bccReceivers;
	// 信件主旨
	private String subject;
	// 信件內容
	private String content;
	
	public ETL_Bean_MailFilter() {
		
	}
	
	public ETL_Bean_MailFilter(List<String> receivers, List<String> ccReceivers, List<String> bccReceivers, String subject, String content) {
		this.receivers = receivers;
		this.ccReceivers = ccReceivers;
		this.bccReceivers = bccReceivers;
		this.subject = subject;
		this.content = content;
	}

	public Long getPkey() {
		return Pkey;
	}

	public void setPkey(Long pkey) {
		Pkey = pkey;
	}

	public List<String> getReceivers() {
		return receivers;
	}

	public void setReceivers(List<String> receivers) {
		this.receivers = receivers;
	}

	public List<String> getCcReceivers() {
		return ccReceivers;
	}

	public void setCcReceivers(List<String> ccReceivers) {
		this.ccReceivers = ccReceivers;
	}

	public List<String> getBccReceivers() {
		return bccReceivers;
	}

	public void setBccReceivers(List<String> bccReceivers) {
		this.bccReceivers = bccReceivers;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
