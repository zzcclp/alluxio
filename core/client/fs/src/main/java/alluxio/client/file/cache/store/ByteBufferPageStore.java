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

package alluxio.client.file.cache.store;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.exception.PageNotFoundException;
import alluxio.exception.status.ResourceExhaustedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@NotThreadSafe
public class ByteBufferPageStore implements PageStore {
    private static final Logger LOG = LoggerFactory.getLogger(ByteBufferPageStore.class);
    // TODO: add to Errno
    private static final String ERROR_NO_MEMORY_LEFT = "No memory left";
    private final String mRoot;
    private final long mPageSize;
    private final long mCapacity;
    private final int mFileBuckets;

    private ConcurrentHashMap<String, byte[]> mPageStoreMap = null;

    public ByteBufferPageStore(ByteBufferPageStoreOptions options) {
        mRoot = options.getRootDir();
        mPageSize = options.getPageSize();
        mCapacity = (long) (options.getCacheSize() / (1 + options.getOverheadRatio()));
        mFileBuckets = options.getFileBuckets();
        mPageStoreMap = new ConcurrentHashMap<>();
        // normalize the path to deal with trailing slash
        Path rootDir = Paths.get(mRoot);
    }

    @Override
    public void put(PageId pageId, byte[] page) throws ResourceExhaustedException, IOException {
        String pageKey = getKeyFromPageId(pageId);
        try {
            byte[] mPage = new byte[page.length];
            System.arraycopy(page, 0, mPage, 0, page.length);
            mPageStoreMap.put(pageKey, mPage);
        } catch (Exception e) {
            if (e.getMessage().contains(ERROR_NO_MEMORY_LEFT)) {
                throw new ResourceExhaustedException(
                        String.format("%s is full, configured with %d bytes", "Memory", mCapacity),
                        e);
            }
        }
    }

    @Override
    public int get(PageId pageId, int pageOffset, int bytesToRead, byte[] buffer, int bufferOffset)
            throws IOException, PageNotFoundException {
        Preconditions.checkArgument(buffer != null, "buffer is null");
        Preconditions.checkArgument(pageOffset >= 0, "page offset should be non-negative");
        Preconditions.checkArgument(buffer.length >= bufferOffset,
                "page offset %s should be " + "less or equal than buffer length %s", bufferOffset,
                buffer.length);
        String pageKey = getKeyFromPageId(pageId);
        if (!mPageStoreMap.containsKey(pageKey)) {
            throw new PageNotFoundException(pageKey);
        }
        byte[] mPage = mPageStoreMap.get(pageKey);
        Preconditions.checkArgument(pageOffset <= mPage.length, "page offset %s exceeded page size %s",
                pageOffset, mPage.length);
        int bytesLeft = (int) Math.min(mPage.length - pageOffset, buffer.length - bufferOffset);
        bytesLeft = Math.min(bytesLeft, bytesToRead);
        System.arraycopy(mPage, pageOffset, buffer, bufferOffset, bytesLeft);
        return bytesLeft;
    }

    @Override
    public void delete(PageId pageId) throws IOException, PageNotFoundException {
        String pageKey = getKeyFromPageId(pageId);
        if (!mPageStoreMap.containsKey(pageKey)) {
            throw new PageNotFoundException(pageKey);
        }
        LOG.info("Remove cached page, size: {}", mPageStoreMap.size());
        mPageStoreMap.remove(pageKey);
    }

    /**
     * @param pageId page Id
     * @return the key to this page
     */
    @VisibleForTesting
    public String getKeyFromPageId(PageId pageId) {
        // TODO(feng): encode fileId with URLEncoder to escape invalid characters for file name
        // Key is : bucket(uint)_file_id(str)_page_idx(ulong)
        StringBuffer result = new StringBuffer(getFileBucket(pageId.getFileId()));
        result.append("_").append(pageId.getFileId())
                .append("_").append(Long.toString(pageId.getPageIndex()));
        return result.toString();
    }

    private String getFileBucket(String fileId) {
        return Integer.toString(Math.floorMod(fileId.hashCode(), mFileBuckets));
    }

    @Override
    public void close() {
        mPageStoreMap.clear();
        mPageStoreMap = null;
    }

    @Override
    public Stream<PageInfo> getPages() throws IOException {
        return (new ArrayList<PageInfo>(0)).stream();
    }

    @Override
    public long getCacheSize() {
        return mCapacity;
    }
}
