package testdemo.demo;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class MultipartAppendTest {
    public static void main(String[] args) {
        String endpoint = "oss-cn-chengdu.aliyuncs.com";
        String accessKeyId = "";
        String accessKeySecret = "";
        String bucketName = "oss-other-user";
        String objectName = "append1091.txt";
        OSS ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);

        try{
            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, objectName);
            InitiateMultipartUploadResult upresult = ossClient.initiateMultipartUpload(request);
            String uploadId = upresult.getUploadId();


            ObjectListing objectListing = ossClient.listObjects(bucketName, "mutipart-");
            List<OSSObjectSummary> sums = objectListing.getObjectSummaries();
            int count=0;
            for (OSSObjectSummary s : sums) {
                if (s.getKey().endsWith("_SUCCESS") || s.getSize()==0){
                    continue;
                }
                count++;
                System.out.println("count:"+count);

                InputStream input = getFSDataInputStream(ossClient,"/"+s.getKey(), bucketName);
                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setBucketName(bucketName);
                uploadPartRequest.setKey(objectName);
                uploadPartRequest.setUploadId(uploadId);
                uploadPartRequest.setInputStream(input);
                uploadPartRequest.setPartSize(input.available());
                uploadPartRequest.setPartNumber(count + 1);

                UploadPartResult uploadPartResult = ossClient.uploadPart(uploadPartRequest);
                System.out.println("uploadId:"+uploadPartRequest.getUploadId()+"  ETag:"+uploadPartResult.getPartETag().getETag()+"  PartNumber:"+uploadPartRequest.getPartNumber()+"  PartSize:"+uploadPartRequest.getPartSize());
            }


            CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucketName, objectName, uploadId, null);


            Map<String, String> headers = new HashMap<String, String>();
            headers.put("x-oss-complete-all","yes");
            completeMultipartUploadRequest.setHeaders(headers);
            CompleteMultipartUploadResult completeMultipartUploadResult = ossClient.completeMultipartUpload(completeMultipartUploadRequest);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static InputStream getFSDataInputStream(OSS ossClient, String srcFile, String bucketName) throws IOException {
        srcFile=srcFile.substring(srcFile.indexOf("/")+1);

        ByteArrayInputStream inputStream =null;
        OSSObject ossObject = null;
        try {
            ossObject = ossClient.getObject(bucketName, srcFile);
            InputStream is = ossObject.getObjectContent();
            ByteArrayOutputStream byteArrayOutputStream = copyStream(is);
            inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if (ossObject != null) {
                ossObject.close();
            }
        }
        return inputStream;
    }


    public static ByteArrayOutputStream copyStream(InputStream fsDataInputStream) throws Exception {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = fsDataInputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }

        return result;
    }
}
