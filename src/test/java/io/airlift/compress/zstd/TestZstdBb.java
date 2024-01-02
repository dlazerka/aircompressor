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

import io.airlift.compress.*;
import io.airlift.compress.benchmark.DataSet;
import io.airlift.compress.thirdparty.ZstdJniCompressor;
import io.airlift.compress.thirdparty.ZstdJniDecompressor;
import io.trino.hadoop.$internal.com.microsoft.azure.storage.table.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static io.airlift.compress.Util.readResource;
import static io.airlift.compress.Util.readResourceBb;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestZstdBb
        extends AbstractTestCompressionBb
{
    @Override
    protected Compressor getCompressor()
    {
        return new ZstdCompressor();
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

    // Ideally, this should be covered by super.testDecompressWithOutputPadding(...), but the data written by the native
    // compressor doesn't include checksums, so it's not a comprehensive test. The dataset for this test has a checksum.
    @Test(enabled = false)
    public void testDecompressWithOutputPaddingAndChecksum()
            throws IOException
    {
        int padding = 1021;

        byte[] compressed = readResource("data/zstd/with-checksum.zst");
        byte[] uncompressed = readResource("data/zstd/with-checksum");

        byte[] output = new byte[uncompressed.length + padding * 2]; // pre + post padding
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressed.length, output, padding, output.length - padding);

        assertByteArraysEqual(uncompressed, 0, uncompressed.length, output, padding, decompressedSize);
    }

    @Test
    public void testConcatenatedFrames()
            throws IOException
    {
        ByteBuffer compressed = readResourceBb("data/zstd/multiple-frames.zst");
        ByteBuffer uncompressed = readResourceBb("data/zstd/multiple-frames");

        ByteBuffer output = ByteBuffer.allocate(uncompressed.remaining()).order(LITTLE_ENDIAN);
        getDecompressor().decompress(compressed, output);

        output.rewind();
        assertByteBufferEqual(uncompressed, output);
    }

    @Test
    public void testInvalidSequenceOffset()
            throws IOException
    {
        ByteBuffer compressed = readResourceBb("data/zstd/offset-before-start.zst");
        ByteBuffer output = ByteBuffer.allocate(compressed.remaining() * 10).order(LITTLE_ENDIAN);

        assertThatThrownBy(() -> getDecompressor().decompress(compressed, output))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageStartingWith("Input is corrupted: offset=894");
    }

    @Test(enabled = false)
    public void testSmallLiteralsAfterIncompressibleLiterals()
            throws IOException
    {
        // Ensure the compressor doesn't try to reuse a huffman table that was created speculatively for a previous block
        // which ended up emitting raw literals due to insufficient gain
        Compressor compressor = getCompressor();

        byte[] original = readResource("data/zstd/small-literals-after-incompressible-literals");
        int maxCompressLength = compressor.maxCompressedLength(original.length);

        byte[] compressed = new byte[maxCompressLength];
        int compressedSize = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        byte[] decompressed = new byte[original.length];
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);

        assertByteArraysEqual(original, 0, original.length, decompressed, 0, decompressedSize);
    }

    @Test(enabled = false)
    public void testLargeRle()
            throws IOException
    {
        // Dataset that produces an RLE block with 3-byte header

        Compressor compressor = getCompressor();

        byte[] original = readResource("data/zstd/large-rle");
        int maxCompressLength = compressor.maxCompressedLength(original.length);

        byte[] compressed = new byte[maxCompressLength];
        int compressedSize = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        byte[] decompressed = new byte[original.length];
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);

        assertByteArraysEqual(original, 0, original.length, decompressed, 0, decompressedSize);
    }

    @Test(enabled = false)
    public void testIncompressibleData()
            throws IOException
    {
        // Incompressible data that would require more than maxCompressedLength(...) to store

        Compressor compressor = getCompressor();

        byte[] original = readResource("data/zstd/incompressible");
        int maxCompressLength = compressor.maxCompressedLength(original.length);

        byte[] compressed = new byte[maxCompressLength];
        int compressedSize = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        byte[] decompressed = new byte[original.length];
        int decompressedSize = getDecompressor().decompress(compressed, 0, compressedSize, decompressed, 0, decompressed.length);

        assertByteArraysEqual(original, 0, original.length, decompressed, 0, decompressedSize);
    }

    @Test(enabled = false)
    public void testMaxCompressedSize()
    {
        assertEquals(new ZstdCompressor().maxCompressedLength(0), 64);
        assertEquals(new ZstdCompressor().maxCompressedLength(64 * 1024), 65_824);
        assertEquals(new ZstdCompressor().maxCompressedLength(128 * 1024), 131_584);
        assertEquals(new ZstdCompressor().maxCompressedLength(128 * 1024 + 1), 131_585);
    }

    // test over data sets, should the result depend on input size or its compressibility
    @Test(dataProvider = "data")
    public void testGetDecompressedSize(DataSet dataSet)
    {
        Compressor compressor = getCompressor();
        byte[] originalUncompressed = dataSet.getUncompressed();
        ByteBuffer compressed = ByteBuffer.allocate(compressor.maxCompressedLength(originalUncompressed.length)).order(LITTLE_ENDIAN);

        int compressedLength = compressor.compress(originalUncompressed, 0, originalUncompressed.length, compressed.array(), 0, compressed.remaining());

        assertEquals(ZstdDecompressorBb.getDecompressedSize(compressed, 0, compressedLength), originalUncompressed.length);

        int padding = 10;
        ByteBuffer compressedWithPadding = ByteBuffer.allocate(compressedLength + padding).order(LITTLE_ENDIAN);
        Arrays.fill(compressedWithPadding.array(), (byte) 42);
        System.arraycopy(compressed.array(), 0, compressedWithPadding.array(), padding, compressedLength);

        assertEquals(ZstdDecompressorBb.getDecompressedSize(compressedWithPadding, padding, compressedLength), originalUncompressed.length);
    }

    @Test
    public void testVerifyMagicInAllFrames()
            throws IOException
    {
        ByteBuffer compressed = readResourceBb("data/zstd/bad-second-frame.zst");
        ByteBuffer uncompressed = readResourceBb("data/zstd/multiple-frames");
        ByteBuffer output = ByteBuffer.allocate(uncompressed.remaining()).order(LITTLE_ENDIAN);
        assertThatThrownBy(() -> getDecompressor().decompress(compressed, output))
                .isInstanceOf(MalformedInputException.class)
                .hasMessageStartingWith("Invalid magic prefix");
    }

    @Test
    public void testDecompressIsMissingData()
    {
        ByteBuffer input = ByteBuffer.wrap(new byte[]{40, -75, 47, -3, 32, 0, 1, 0});
        ByteBuffer output = ByteBuffer.allocate(1024).order(LITTLE_ENDIAN);
        assertThatThrownBy(() -> getDecompressor().decompress(input, output))
                .matches(e -> e instanceof MalformedInputException || e instanceof UncheckedIOException)
                .hasMessageContaining("Not enough input bytes");
    }
}
