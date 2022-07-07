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

package alluxio.client.file;

import static org.junit.Assert.fail;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.resource.CloseableResource;

import com.google.common.io.Closer;
import org.junit.Test;

import java.util.function.Supplier;

/**
 * Tests {@link FileSystemContext}.
 */
public final class FileSystemContextTest {

  private final FileSystemContext mFileSystemContext =
          FileSystemContext.create(Configuration.global());

  @Test(timeout = 10000)
  public void acquireFileSystemMasterClient() throws Exception {
    acquireAtMaxLimit(mFileSystemContext::acquireMasterClientResource);
  }

  @Test(timeout = 10000)
  public void acquireBlockMasterClient() throws Exception {
    acquireAtMaxLimit(mFileSystemContext::acquireBlockMasterClientResource);
  }

  /**
   * This test ensures acquiring all the available resources from context blocks further
   * requests for the same resource.
   * It also ensures that resources are available for reuse after they are released
   * by the previous owners. If the test takes longer than 10 seconds, a deadlock most likely
   * occurred preventing the release of resources.
   *
   * @param resourceAcquirer function to acquire resource from pool
   */
  private void acquireAtMaxLimit(Supplier<CloseableResource<?>> resourceAcquirer) throws Exception {
    Closer closer = Closer.create();
    for (int i = 0; i < Configuration
        .getInt(PropertyKey.USER_FILE_MASTER_CLIENT_POOL_SIZE_MAX); i++) {
      // these acquire should proceed without blocking
      closer.register(resourceAcquirer.get());
    }

    // this acquire should block before we close existing resources
    Thread acquireThread = new Thread(new AcquireClient(resourceAcquirer));
    acquireThread.start();

    // Wait for the spawned thread to complete.
    // It shouldn't be able to complete as we haven't released resources yet
    long timeoutMs = Constants.SECOND_MS / 2;
    long start = System.currentTimeMillis();
    acquireThread.join(timeoutMs);
    if (System.currentTimeMillis() - start < timeoutMs) {
      fail("Acquired a master client when the client pool was full.");
    }

    // Release all the clients
    closer.close();

    // Wait for the spawned thread to complete. If it is unable to acquire a master client before
    // the defined timeout, fail.
    timeoutMs = 5 * Constants.SECOND_MS;
    start = System.currentTimeMillis();
    acquireThread.join(timeoutMs);
    if (System.currentTimeMillis() - start >= timeoutMs) {
      fail("Failed to acquire a master client within " + timeoutMs + "ms. Deadlock?");
    }
  }

  private static class AcquireClient implements Runnable {

    private final Supplier<CloseableResource<?>> mCloseableResourceSupplier;

    public AcquireClient(Supplier<CloseableResource<?>> closeableResourceSupplier) {
      mCloseableResourceSupplier = closeableResourceSupplier;
    }

    @Override
    public void run() {
      CloseableResource<?> closeableResource = mCloseableResourceSupplier.get();
      closeableResource.close();
    }
  }
}
