package Bean;

import java.util.List;

public class ETL_Bean_MailFilter {

	// 收件者
	private List<String> receivers;
	// CC群
	private List<String> ccReceivers;
	// BCC群
	private List<String> bccReceivers;
	
	
	// 過濾後收件者
	private List<String> returnReceivers;
	// 過濾後CC群
	private List<String> returnCcReceivers;
	// 過濾後BCC群
	private List<String> returnBccReceivers;
	
	
	public ETL_Bean_MailFilter(List<String> receivers, List<String> ccReceivers, List<String> bccReceivers) {
		this.receivers = receivers;
		this.ccReceivers = ccReceivers;
		this.bccReceivers = bccReceivers;
	}
	
//	public filter
	
	public static void main(String[] args) {
		
		

	}

}
