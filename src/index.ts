import { S3 } from "aws-sdk";
import { Client } from "./client";

let client: Client;

export function getClient(config: S3.ClientConfiguration = {}) {
  if (client) {
    return client;
  }
  const S3Client = new S3(config);
  client = new Client(S3Client);

  return client;
}
