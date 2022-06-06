/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.zstd;

import io.airlift.compress.AbstractTestCompression;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.benchmark.DataSet;
import io.airlift.compress.thirdparty.ZstdJniCompressor;
import io.airlift.compress.thirdparty.ZstdJniDecompressor;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Objects;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.testng.Assert.assertEquals;

public class TestZstdBb
        extends AbstractTestCompression
{
    @Override
    protected Compressor getCompressor()
    {
        return new ZstdCompressorBb();
    }

    @Override
    protected Decompressor getDecompressor()
    {
        return new ZstdDecompressorBb();
    }

    @Override
    protected Compressor getVerifyCompressor()
    {
        return new ZstdJniCompressor(3);
    }

    @Override
    protected Decompressor getVerifyDecompressor()
    {
        return new ZstdJniDecompressor();
    }

    @Test
    public void testIncompressibleData()
            throws IOException {
        // Incompressible data that would require more than maxCompressedLength(...) to store

        Compressor compressor = getCompressor();

        byte[] original = readFileAsBytes("data/zstd/incompressible");
        byte[] original = readFileAsByteBuffer("data/zstd/incompressible");
        int maxCompressLength = compressor.maxCompressedLength(original.length);

        // byte[] compressed = new byte[maxCompressLength];
        var compressed = ByteBuffer.allocate(maxCompressLength);
        int compressedSize = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        byte[] decompressed = new byte[original.length];
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);

        assertByteArraysEqual(original, 0, original.length, decompressed, 0, decompressedSize);
    }
    @Test
    public void testMaxCompressedSize() {
        assertEquals(new ZstdCompressorBb().maxCompressedLength(0), 64);
        assertEquals(new ZstdCompressorBb().maxCompressedLength(64 * 1024), 65_824);
        assertEquals(new ZstdCompressorBb().maxCompressedLength(128 * 1024), 131_584);
        assertEquals(new ZstdCompressorBb().maxCompressedLength(128 * 1024 + 1), 131_585);
    }

    // test over data sets, should the result depend on input size or its compressibility
    @Test(dataProvider = "data")
    public void testGetDecompressedSizeBB(DataSet dataSet) throws IOException {
        Compressor compressor = getCompressor();
        byte[] originalUncompressed = dataSet.getUncompressed();
        byte[] compressed = new byte[compressor.maxCompressedLength(originalUncompressed.length)];
        int compressedLength = compressor.compress(originalUncompressed, 0, originalUncompressed.length, compressed, 0, compressed.length);

        ByteBuffer compressedBb = ByteBuffer.wrap(compressed).order(LITTLE_ENDIAN);

        assertByteArraysEqual(compressed, 0, compressed.length, compressed, 0, compressed.length);

        assertEquals(ZstdDecompressorBb.getDecompressedSize(compressedBb), originalUncompressed.length);
        compressedBb.rewind();

        int padding = 10;
        ByteBuffer compressedWithPadding = ByteBuffer.allocate(compressedLength + padding).order(LITTLE_ENDIAN);
        Arrays.fill(compressedWithPadding.array(), (byte) 42);
        compressedWithPadding.put(compressed, 0, compressedLength + padding);
        compressedWithPadding.rewind();

        assertEquals(ZstdDecompressorBb.getDecompressedSize(compressedWithPadding), originalUncompressed.length);
    }

    private byte[] readFileAsBytes(String path) throws IOException {
        URL url = getClass().getClassLoader().getResource(path);
        Objects.requireNonNull(url, path);
        return Files.readAllBytes(Path.of(url.getFile()));
    }
    private ByteBuffer readFileAsByteBuffer(String path) throws IOException {
        URL url = getClass().getClassLoader().getResource(path);
        Objects.requireNonNull(url, path);
        long size = Files.size(Path.of(url.getFile()));
        assert size <= Integer.MAX_VALUE;
        ByteBuffer contents = ByteBuffer.allocate((int) size).order(LITTLE_ENDIAN);
        try (FileChannel fc = FileChannel.open(Path.of(url.getFile()), StandardOpenOption.READ)) {
            fc.read(contents);
        }
        return contents;
    }
}
