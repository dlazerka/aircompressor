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

import java.nio.ByteBuffer;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ZstdDecompressorBb
        extends ZstdDecompressor
{
    // private final BbZstdFrameDecompressor decompressor = new BbZstdFrameDecompressor();

    public static long getDecompressedSize(ByteBuffer bb)
    {
        return ZstdFrameDecompressorBb.getDecompressedSize(bb);
    }

    private static void verifyRange(byte[] data, int offset, int length)
    {
        requireNonNull(data, "data is null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException(format("Invalid offset or length (%s, %s) in array of length %s", offset, length, data.length));
        }
    }
}
