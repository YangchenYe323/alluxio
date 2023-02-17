/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.fs.io;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;

import org.junit.Test;

public class FileOutStreamClientPoolStressTest extends AbstractFileOutStreamIntegrationTest {
  public final WriteType mWriteType = WriteType.MUST_CACHE;
  public static final int WORKER_CLIENT_POOL_SIZE = 64;

  @Override
  protected void customizeClusterResource(LocalAlluxioClusterResource.Builder resource) {
    super.customizeClusterResource(resource);
    resource.setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false);
    resource.setProperty(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_MAX, WORKER_CLIENT_POOL_SIZE);
    resource.setProperty(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_MIN, 0);
  }

  @Test
  public void writeMoreBlocksThanPoolSize() throws Exception {
    // The file requires one more block than the BlockWorkerClientPool can offer,
    // so without reusing of client, the operation will hang forever.
    int fileLen = BLOCK_SIZE_BYTES * (WORKER_CLIENT_POOL_SIZE + 1);
    String uniquePath = PathUtils.uniqPath();
    AlluxioURI filePath =
            new AlluxioURI(PathUtils.concatPath(uniquePath, "file_" + fileLen + "_" + mWriteType));
    CreateFilePOptions op = CreateFilePOptions.newBuilder().setWriteType(mWriteType.toProto())
            .setRecursive(true).build();
    writeIncreasingBytesToFile(filePath, fileLen, op);
    checkFileInAlluxio(filePath, fileLen);
  }
}
