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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static io.airlift.compress.Util.readResourceAsByteBuffer;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.testng.Assert.assertEquals;

public class TestZstdBb
        extends AbstractTestCompression {
    @Override
    protected ZstdCompressorBb getCompressor() {
        return new ZstdCompressorBb();
    }

    @Override
    protected Decompressor getDecompressor() {
        return new ZstdDecompressorBb();
    }

    @Override
    protected Compressor getVerifyCompressor() {
        return new ZstdJniCompressor(3);
    }

    @Override
    protected Decompressor getVerifyDecompressor() {
        return new ZstdJniDecompressor();
    }

    @Test(dataProvider = "data")
    @Override
    public void testCompress(DataSet testCase) {
        ZstdCompressorBb compressor = getCompressor();

        byte[] originalUncompressed2 = testCase.getUncompressed();
        ByteBuffer originalUncompressed = ByteBuffer.wrap(originalUncompressed2).order(LITTLE_ENDIAN);
        int maxCompressedLength = compressor.maxCompressedLength(originalUncompressed.remaining());
        ByteBuffer compressed = ByteBuffer.allocate(maxCompressedLength).order(LITTLE_ENDIAN);

        // attempt to compress slightly different data to ensure the compressor doesn't keep state
        // between calls that may affect results
/* TODO

        if (originalUncompressed.remaining() > 1) {
            // byte[] output = new byte[compressor.maxCompressedLength(originalUncompressed.length - 1)];
            int maxCompressedLengthTmp = compressor.maxCompressedLength(originalUncompressed.remaining() - 1);
            ByteBuffer outputTmp = ByteBuffer.allocate(maxCompressedLengthTmp).order(LITTLE_ENDIAN);
            // compressor.compress(originalUncompressed, 1, originalUncompressed.length - 1, output, 0, output.length);
            originalUncompressed.position(1);

            compressor.compress(originalUncompressed, outputTmp);
            originalUncompressed.rewind();
        }
*/

        compressor.compress(originalUncompressed, compressed);
        compressed.rewind();

        Path path = FileSystems.getDefault().getPath("bb.bin");
        try (FileChannel fc = FileChannel.open(path, WRITE, CREATE)) {
            ByteBuffer bb = ByteBuffer.wrap(compressed.array(), 0, compressed.capacity());
            fc.write(bb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        verifyCompressedData(
                originalUncompressed.array(),
                compressed.array(),
                compressed.capacity()
        );
    }

    @Test
    public void testIncompressibleData()
            throws IOException {
        // Incompressible data that would require more than maxCompressedLength(...) to store

        Compressor compressor = getCompressor();

        ByteBuffer original = readResourceAsByteBuffer("data/zstd/incompressible");
        int maxCompressLength = compressor.maxCompressedLength(original.remaining());

        // byte[] compressed = new byte[maxCompressLength];
        var compressed = ByteBuffer.allocate(maxCompressLength);
        compressor.compress(original, compressed);
        original.rewind();
        compressed.flip();

        // byte[] decompressed = new byte[original.length];
        ByteBuffer decompressed = ByteBuffer.allocate(original.capacity());
        getDecompressor().decompress(compressed, decompressed);
        decompressed.flip();

        assertByteArraysEqual(original.array(), 0, original.remaining(), decompressed.array(), 0, decompressed.remaining());
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


}
