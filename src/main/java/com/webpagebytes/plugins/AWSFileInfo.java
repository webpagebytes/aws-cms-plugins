package com.webpagebytes.plugins;

import java.util.Map;

import com.webpagebytes.cms.WPBFileInfo;
import com.webpagebytes.cms.WPBFilePath;

public class AWSFileInfo implements WPBFileInfo  {

	private WPBFilePath filePath;
	private String contentType;
	private Long crc32 = 0L;
	private long creationDate;
	private String md5;
	private long size = 0;

	public AWSFileInfo(WPBFilePath filePath, String contentType, long size, String md5, long crc32, long creationDate)
	{
		this.filePath = filePath;
		this.contentType = contentType;
		this.size = size;
		this.md5 = md5;
		this.crc32 = crc32;
		this.creationDate = creationDate;
	}
	public String getContentType() {
		// TODO Auto-generated method stub
		return contentType;
	}

	public long getCrc32() {
		// TODO Auto-generated method stub
		return crc32;
	}

	public long getCreationDate() {
		// TODO Auto-generated method stub
		return creationDate;
	}

	public Map<String, String> getCustomProperties() {
		// TODO Auto-generated method stub
		return null;
	}

	public WPBFilePath getFilePath() {
		// TODO Auto-generated method stub
		return filePath;
	}

	public String getMd5() {
		// TODO Auto-generated method stub
		return md5;
	}

	public String getProperty(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	public long getSize() {
		// TODO Auto-generated method stub
		return size;
	}

	public void setContentType(String contentType) {
		// TODO Auto-generated method stub
		this.contentType = contentType;
	}

	public void setCustomProperties(Map<String, String> arg0) {
	}

	public void setProperty(String arg0, String arg1) {
		
	}

}
