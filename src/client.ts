import { S3 } from "aws-sdk";
import { Readable } from "stream";

export class Client {
  private _s3: S3;
  private readonly CHUNK_SIZE = 5 * 1024 * 1024; // 5MB

  constructor(config: S3.ClientConfiguration) {
    this._s3 = new S3(config);
  }

  async uploadFile(
    bucket: string,
    key: string,
    body: Buffer | Uint8Array | Blob | string | Readable,
    options?: S3.PutObjectRequest,
    useMultipart?: boolean
  ): Promise<S3.PutObjectOutput | S3.CompleteMultipartUploadOutput> {
    if (useMultipart) {
      return this.uploadMultipart(bucket, key, body as Readable, options);
    }

    const params: S3.PutObjectRequest = {
      Bucket: bucket,
      Key: key,
      Body: body,
      ...options,
    };

    return this._s3.putObject(params).promise();
  }

  private async uploadMultipart(
    bucket: string,
    key: string,
    body: Readable,
    options?: S3.PutObjectRequest
  ): Promise<S3.CompleteMultipartUploadOutput> {
    const createMultipart = await this._s3
      .createMultipartUpload({ Bucket: bucket, Key: key, ...options })
      .promise();
    const uploadId = createMultipart.UploadId;

    const partPromises: Array<Promise<S3.UploadPartOutput>> = [];
    let partNumber = 0;

    while (true) {
      const chunk = body.read(this.CHUNK_SIZE);
      if (chunk === null) break;

      partNumber++;
      const partUpload = this._s3
        .uploadPart({
          Bucket: bucket,
          Key: key,
          PartNumber: partNumber,
          UploadId: uploadId!,
          Body: chunk,
        })
        .promise();
      partPromises.push(partUpload);
    }

    const parts = await Promise.all(partPromises);
    const sortedParts = parts.map((part, index) => ({
      PartNumber: index + 1,
      ETag: part.ETag!,
    }));

    return this._s3
      .completeMultipartUpload({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId!,
        MultipartUpload: { Parts: sortedParts },
      })
      .promise();
  }

  async getPresignedUploadURL(
    bucket: string,
    key: string,
    expires: number = 900
  ): Promise<string> {
    const params: S3.PresignedPost.Params = {
      Bucket: bucket,
      Fields: {
        key: key,
      },
      Expires: expires,
    };

    const signedUrl = await this._s3.createPresignedPost(params);
    return signedUrl.url;
  }

  /**
   * Downloads a file from a specified S3 bucket and key.
   * @param {string} bucket - The name of the bucket.
   * @param {string} key - The key (path) of the file in the bucket.
   * @param {boolean} [usePresignedUrl] - If true, returns a pre-signed URL for downloading. Otherwise, returns the file data.
   * @param {number} [expires] - Expiration time for the pre-signed URL in seconds. Default is 15 minutes (900 seconds).
   * @returns {Promise<Buffer | string>} - Returns the file data as a Buffer if usePresignedUrl is false. Returns a pre-signed URL if usePresignedUrl is true.
   */
  async downloadFile(bucket: string, key: string): Promise<Buffer | string> {
    const params: S3.GetObjectRequest = {
      Bucket: bucket,
      Key: key,
    };

    const response = await this._s3.getObject(params).promise();
    return response.Body as Buffer;
  }

  async getPresignedDownloadURL(
    bucket: string,
    key: string,
    expires: number = 900
  ): Promise<string> {
    const params: S3.GetObjectRequest = {
      Bucket: bucket,
      Key: key,
    };

    return this._s3.getSignedUrlPromise("getObject", {
      ...params,
      Expires: expires,
    });
  }

  /**
   * Lists all objects in a bucket, optionally filtered by a prefix.
   * @param {string} bucket - The name of the bucket.
   * @param {string} [prefix] - An optional prefix to filter the results.
   * @returns {Promise<S3.ObjectList>} - A list of objects in the bucket.
   */
  async listObjects(bucket: string, prefix?: string): Promise<S3.ObjectList> {
    const params: S3.ListObjectsV2Request = {
      Bucket: bucket,
      Prefix: prefix,
    };

    const response = await this._s3.listObjectsV2(params).promise();
    return response.Contents || [];
  }

  /**
   * Deletes a file from a specified S3 bucket and key.
   * @param {string} bucket - The name of the bucket.
   * @param {string} key - The key (path) of the file in the bucket.
   * @returns {Promise<S3.DeleteObjectOutput>} - The result of the delete operation.
   */
  async deleteFile(
    bucket: string,
    key: string
  ): Promise<S3.DeleteObjectOutput> {
    const params: S3.DeleteObjectRequest = {
      Bucket: bucket,
      Key: key,
    };

    return this._s3.deleteObject(params).promise();
  }
}
