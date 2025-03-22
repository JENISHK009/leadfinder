import AWS from 'aws-sdk';

const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
});

export const uploadFileToS3 = async (fileBuffer, fileName, bucketName) => {
  const params = {
    Bucket: bucketName,
    Key: fileName,
    Body: fileBuffer,
    ContentType: 'text/csv',
  };

  // Upload the file
  await s3.upload(params).promise();

  // Generate a pre-signed URL for the uploaded file
  const signedUrl = await s3.getSignedUrlPromise('getObject', {
    Bucket: bucketName,
    Key: fileName,
    Expires: 3600, // URL expires in 1 hour (adjust as needed)
  });

  return { fileUrl: signedUrl };
};