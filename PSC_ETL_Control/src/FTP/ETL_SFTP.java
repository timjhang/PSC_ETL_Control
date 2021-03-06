package FTP;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import Control.ETL_C_Profile;

public class ETL_SFTP {
	public final static String FILE_TEMP = "C:/ETL/DM/";



	public static ChannelSftp connect(String host, int port, String username, String password) {
		ChannelSftp sftp = null;
		try {
			JSch jsch = new JSch();
			jsch.getSession(username, host, port);
			Session sshSession = jsch.getSession(username, host, port);
//			System.out.println("Session created");
			sshSession.setPassword(password);
			Properties sshConfig = new Properties();
			sshConfig.put("StrictHostKeyChecking", "no");
			sshSession.setConfig(sshConfig);
			sshSession.connect();
//			System.out.println("Session connected.");
//			System.out.println("Opening Channel.");
			Channel channel = sshSession.openChannel("sftp");
			channel.connect();
			sftp = (ChannelSftp) channel;
//			System.out.println("Connected to " + host);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sftp;
	}
	
	public static List<String> listFiles(String host, int port, String username, String password, String directory) throws SftpException, JSchException {
		ChannelSftp sftp = connect(host, port, username, password);
		List<String> result = new ArrayList<String>();
		List<ChannelSftp.LsEntry> list = sftp.ls(directory);
		
		for (ChannelSftp.LsEntry entry : list) {
			if (!".".equals(entry.getFilename()) && !"..".equals(entry.getFilename())) {
				result.add(entry.getFilename());
			}
		}
		
		Session session = sftp.getSession();
		sftp.disconnect();
		session.disconnect();
	
		return result;
	}
	
	public static boolean upload(String hostName, String port, String username, String password, 
			String localFilePath, String remoteFilePath) {

		File f = new File(localFilePath);
		if (!f.exists()) {
			System.out.println("Local file " + remoteFilePath + " not found. Upload failure.");
			
			return false;
		}

		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create local file object
			FileObject localFile = manager.resolveFile(f.getAbsolutePath());

			// Create remote file object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, remoteFilePath), createDefaultOptions());

			// Copy local file to sftp server
			remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);

			System.out.println("File upload success");
			
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			manager.close();
		}
	}

	public static boolean download(String hostName, String port, String username, String password, 
			String localFilePath, String remoteFilePath) {

		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();
			
			String downloadFilePath = localFilePath;
//			String downloadFilePath = localFilePath.substring(0, localFilePath.lastIndexOf("."))
//					+ localFilePath.substring(localFilePath.lastIndexOf("."), localFilePath.length());

			System.out.println("downloadFilePath = " + downloadFilePath);// test temp

			// Create local file object
			FileObject localFile = manager.resolveFile(downloadFilePath);

			// Create remote file object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, remoteFilePath), createDefaultOptions());
			
			if (remoteFile.exists()) {
				// Copy local file to sftp server
				localFile.copyFrom(remoteFile, Selectors.SELECT_SELF);
	
				System.out.println("File download success");
				
				//成功回傳true
				return true;
			} else {
				
				System.out.println("Remote file " + remoteFilePath + " not found. Download failure.");
				
				// 找不到檔案回傳false
				return false;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			
			// 失敗回傳false
			return false;
		} finally {
			manager.close();
		}
	}

	public static boolean delete(String hostName, String port, String username, String password, String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, remoteFilePath), createDefaultOptions());

			if (remoteFile.exists()) {
				remoteFile.delete();
				System.out.println("Delete remote file success.");
				
				// 成功回傳true
				return true;
			} else {
				System.out.println("Remote file " + remoteFilePath + " not found. Delete failure.");
				
				// 失敗回傳false
				return false;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			// 失敗回傳false
			return false;
		} finally {
			manager.close();
		}
	}
	
	public static boolean exist(String hostName, String port, String username, String password, String remoteFilePath) {
		StandardFileSystemManager manager = new StandardFileSystemManager();

		try {
			manager.init();

			// Create remote object
			FileObject remoteFile = manager.resolveFile(
					createConnectionString(hostName, port, username, password, remoteFilePath), createDefaultOptions());

			System.out.println("File exist: " + remoteFile.exists());

			return remoteFile.exists();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			manager.close();
		}
	}

	private static String createConnectionString(String hostName, String port, String username, String password,
			String remoteFilePath) {
		// result: "sftp://user:123456@domainname.com/resume.pdf
		return "sftp://" + username + ":" + password + "@" + hostName + ":" + port + "/" + remoteFilePath;
	}
	
	
	public static void moveFile(String host, String port, String username, String password, String fromDirectory,
			String toDirectory, List<String> fileNameList) {

		for (String fileName : fileNameList) {

			boolean isSuccess = false;

			isSuccess = download(host, String.valueOf(port), username, password, ETL_SFTP.FILE_TEMP + fileName,
					fromDirectory + fileName);

			if (isSuccess) {
				isSuccess = upload(host, String.valueOf(port), username, password, ETL_SFTP.FILE_TEMP + fileName,
						toDirectory + fileName);
				if (isSuccess) {
					delete(host, port, username, password, fromDirectory + fileName);
				} else {
					System.out.println("上傳錯誤");
				}
			} else {
				System.out.println("下載錯誤");
			}
		}

	}

	private static FileSystemOptions createDefaultOptions() throws FileSystemException {
		// Create SFTP options
		FileSystemOptions opts = new FileSystemOptions();

		// SSH Key checking
		SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(opts, "no");

		// Root directory set to user home
		SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts, true);

		// Timeout is count by Milliseconds
		SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

		return opts;
	}

}
