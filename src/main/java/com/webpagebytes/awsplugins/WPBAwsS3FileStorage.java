package com.webpagebytes.awsplugins;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Map;
import java.util.zip.CRC32;

import javax.xml.bind.DatatypeConverter;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.webpagebytes.awsplugins.AWSFileInfo;
import com.webpagebytes.cms.WPBFileInfo;
import com.webpagebytes.cms.WPBFilePath;
import com.webpagebytes.cms.exception.WPBIOException;

public class WPBAwsS3FileStorage implements com.webpagebytes.cms.WPBFileStorage {

	public static final String CONFIG_ACCESS_KEY = "accessKey";
	public static final String CONFIG_SECRET_KEY = "secretKey";
	public static final String CONFIG_PROTOCOL = "protocol";
	public static final String CONFIG_ENDPOINT = "endpoint";
	public static final String CONFIG_BUCKET = "bucket";
	public static final String CONFIG_WPB_PUBLIC_PATH = "publicBasePath";
	
	protected AmazonS3Client s3client;
	protected String aws_bucket;
	protected String publicBasePath;
	
	public WPBAwsS3FileStorage()
	{

	}
	
	public void initialize(Map<String, String> params) throws WPBIOException
	{
		String accessKey = params.get(CONFIG_ACCESS_KEY);
		String secretkey = params.get(CONFIG_SECRET_KEY);
		BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretkey);
		
		ClientConfiguration clientConfig = new ClientConfiguration();
		
		Protocol protocol = Protocol.valueOf(params.get(CONFIG_PROTOCOL));
		if (protocol != null)
		{
			clientConfig.setProtocol(protocol);
		}
		
		s3client = new AmazonS3Client(awsCredentials, clientConfig);		
		String endpoint = params.get(CONFIG_ENDPOINT);
		if (endpoint != null)
		{
			s3client.setEndpoint(endpoint);
		}
		aws_bucket = params.get(CONFIG_BUCKET);
		
		publicBasePath = params.get(CONFIG_WPB_PUBLIC_PATH);
		if (publicBasePath != null && publicBasePath.length()>0 && ! publicBasePath.endsWith("//"))
		{
			publicBasePath += "//";
		}
	}
	
	public void storeFile(InputStream is, WPBFilePath file) throws IOException {
		
		
		MessageDigest md = null;
		try 
		{
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e)
		{
			throw new IOException("Cannot calculate md5 to store the file", e);
		}

		File tempFile = File.createTempFile("wpbaws", null);
		OutputStream os = new FileOutputStream(tempFile);
		BufferedOutputStream bfos = new BufferedOutputStream(os, 4096);
		CRC32 crc = new CRC32();
		byte[] buffer = new byte[4096];
		
		int count = 0;
		long size = 0L;
		while ((count = is.read(buffer)) != -1)
		{
			size += count;
			bfos.write(buffer, 0, count);
			crc.update(buffer, 0, count);
			md.update(buffer, 0, count);
		}
		bfos.flush();
		bfos.close();
		os.close();
		
		InputStream isFile = new FileInputStream(tempFile);
		
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(size);
		metadata.setContentMD5(DatatypeConverter.printBase64Binary(md.digest()));
		metadata.addUserMetadata("crc32", String.valueOf(crc.getValue()));
		metadata.setContentType("application/octet-stream");
		metadata.setLastModified(new Date());
		PutObjectResult result = s3client.putObject(aws_bucket, file.getPath(), isFile, metadata);
		
		isFile.close();
		
		tempFile.delete();
		
	}

	public WPBFileInfo getFileInfo(WPBFilePath file) throws IOException {
		// TODO Auto-generated method stub
		
		ObjectMetadata metadata = s3client.getObjectMetadata(aws_bucket, file.getPath());
		Map<String,String> userMeta = metadata.getUserMetadata();
		long crc32 = Long.valueOf(userMeta.get("crc32"));
		return new AWSFileInfo(file, metadata.getContentType(), metadata.getContentLength(), metadata.getContentMD5(), crc32 , metadata.getLastModified().getTime());
	}

	public boolean deleteFile(WPBFilePath file) throws IOException {
		// TODO Auto-generated method stub
		try
		{
			s3client.deleteObject(aws_bucket, file.getPath());
		} catch (Exception e)
		{
			
		}
		return false;
	}

	public InputStream getFileContent(WPBFilePath file) throws IOException {
		// TODO Auto-generated method stub
		S3Object object = s3client.getObject(aws_bucket, file.getPath());
		return object.getObjectContent();
	}

	public void updateFileCustomProperties(WPBFilePath file,
			Map<String, String> customProps) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void updateContentType(WPBFilePath file, String contentType)
			throws IOException {
		ObjectMetadata meta = s3client.getObjectMetadata(aws_bucket, file.getPath());
		meta.setContentType(contentType);
		
		final CopyObjectRequest request = new CopyObjectRequest(aws_bucket, file.getPath(), aws_bucket, file.getPath())
        .withSourceBucketName( aws_bucket )
        .withSourceKey(file.getPath())
        .withNewObjectMetadata(meta);

         s3client.copyObject(request);
	}

	public String getPublicFileUrl(WPBFilePath file) {
		// TODO Auto-generated method stub
		
		String filePath = file.getPath();
		
		if (filePath.startsWith("//"))
		{
			filePath = filePath.substring(1);
		}
		return publicBasePath + filePath;
	}

}
