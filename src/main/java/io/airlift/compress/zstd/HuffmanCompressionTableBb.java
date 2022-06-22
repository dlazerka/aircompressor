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
import java.util.Arrays;

import static io.airlift.compress.zstd.Huffman.*;
import static io.airlift.compress.zstd.Util.checkArgument;
import static io.airlift.compress.zstd.Util.minTableLog;

final class HuffmanCompressionTableBb extends HuffmanCompressionTable
{
    private int maxSymbol;
    private int maxNumberOfBits;

    public HuffmanCompressionTableBb(int capacity) {
        super(capacity);
    }

    public int write(ByteBuffer outputBase, int outputAddress, int outputSize, HuffmanTableWriterWorkspace workspace)
    {
        byte[] weights = workspace.weights;

        int output = outputAddress;

        int maxNumberOfBits = this.maxNumberOfBits;
        int maxSymbol = this.maxSymbol;

        // convert to weights per RFC 8478 section 4.2.1
        for (int symbol = 0; symbol < maxSymbol; symbol++) {
            int bits = numberOfBits[symbol];

            if (bits == 0) {
                weights[symbol] = 0;
            }
            else {
                weights[symbol] = (byte) (maxNumberOfBits + 1 - bits);
            }
        }

        // attempt weights compression by FSE
        int size = compressWeights(outputBase, output + 1, outputSize - 1, weights, maxSymbol, workspace);

        if (maxSymbol > 127 && size > 127) {
            // This should never happen. Since weights are in the range [0, 12], they can be compressed optimally to ~3.7 bits per symbol for a uniform distribution.
            // Since maxSymbol has to be <= MAX_SYMBOL (255), this is 119 bytes + FSE headers.
            throw new AssertionError();
        }

        if (size != 0 && size != 1 && size < maxSymbol / 2) {
            // Go with FSE only if:
            //   - the weights are compressible
            //   - the compressed size is better than what we'd get with the raw encoding below
            //   - the compressed size is <= 127 bytes, which is the most that the encoding can hold for FSE-compressed weights (see RFC 8478 section 4.2.1.1). This is implied
            //     by the maxSymbol / 2 check, since maxSymbol must be <= 255
            // UNSAFE.putByte(outputBase, output, (byte) size);
            outputBase.put(output, (byte) size);
            return size + 1; // header + size
        }
        else {
            // Use raw encoding (4 bits per entry)

            // #entries = #symbols - 1 since last symbol is implicit. Thus, #entries = (maxSymbol + 1) - 1 = maxSymbol
            int entryCount = maxSymbol;

            size = (entryCount + 1) / 2;  // ceil(#entries / 2)
            checkArgument(size + 1 /* header */ <= outputSize, "Output size too small"); // 2 entries per byte

            // encode number of symbols
            // header = #entries + 127 per RFC
            // UNSAFE.putByte(outputBase, output, (byte) (127 + entryCount));
            outputBase.put(output, (byte) (127 + entryCount));
            output++;

            weights[maxSymbol] = 0; // last weight is implicit, so set to 0 so that it doesn't get encoded below
            for (int i = 0; i < entryCount; i += 2) {
                outputBase.put(output, (byte) ((weights[i] << 4) + weights[i + 1]));
                output++;
            }

            return output - outputAddress;
        }
    }
}
