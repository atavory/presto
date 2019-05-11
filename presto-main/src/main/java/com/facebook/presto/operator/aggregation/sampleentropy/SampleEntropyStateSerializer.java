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
package com.facebook.presto.operator.aggregation.sampleentropy;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.security.InvalidParameterException;

import static com.facebook.presto.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

public class SampleEntropyStateSerializer
        implements AccumulatorStateSerializer<SampleEntropyState>
{
    private static final int MAX_BYTES_FOR_METHOD = 10;

    public static SampleEntropyStateStrategy create(
            String method,
            long bucketCount,
            double min,
            double max)
    {
        if (method.equalsIgnoreCase("mle")) {
            return new SampleEntropyStateMLEStrategy(bucketCount, min, max);
        }

        if (method.equalsIgnoreCase("jacknife")) {
            return new SampleEntropyStateJacknifeStrategy(bucketCount, min, max);
        }

        throw new InvalidParameterException(String.format("unknown method %s", method));
    }

    public static void validate(String method, SampleEntropyStateStrategy strategy)
    {
        if (method.equalsIgnoreCase("mle")) {
            if (!(strategy instanceof SampleEntropyStateMLEStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        if (method.equalsIgnoreCase("jacknife")) {
            if (!(strategy instanceof SampleEntropyStateJacknifeStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        throw new InvalidParameterException("unknown method");
    }

    public static void combine(SampleEntropyStateStrategy target, SampleEntropyStateStrategy source)
    {
        if (target instanceof SampleEntropyStateMLEStrategy) {
            if (!(source instanceof SampleEntropyStateMLEStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((SampleEntropyStateMLEStrategy) target).mergeWith((SampleEntropyStateMLEStrategy) source);
            return;
        }

        if (target instanceof SampleEntropyStateJacknifeStrategy) {
            if (!(source instanceof SampleEntropyStateJacknifeStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((SampleEntropyStateJacknifeStrategy) target).mergeWith((SampleEntropyStateJacknifeStrategy) source);
            return;
        }

        throw new InvalidParameterException("unknown strategy combination");
    }

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(SampleEntropyState state, BlockBuilder out)
    {
        final int requiredBytes =
                SizeOf.SIZE_OF_INT + // Method
                (state.getStrategy() == null ? 0 : state.getStrategy().getRequiredBytesForSerialization());

        final SampleEntropyStateStrategy strategy = state.getStrategy();

        SliceOutput sliceOut = Slices.allocate(requiredBytes).getOutput();

        if (strategy == null) {
            sliceOut.appendInt(0);
        }
        else if (strategy instanceof SampleEntropyStateMLEStrategy) {
            sliceOut.appendInt(1);
        }
        else if (strategy instanceof SampleEntropyStateJacknifeStrategy) {
            sliceOut.appendInt(2);
        }
        else {
            throw new InvalidParameterException("unknown method in serialize");
        }

        if (strategy != null) {
            strategy.serialize(sliceOut);
        }

        VARBINARY.writeSlice(out, sliceOut.getUnderlyingSlice());
    }

    @Override
    public void deserialize(
            Block block,
            int index,
            SampleEntropyState state)
    {
        final SliceInput input = VARBINARY.getSlice(block, index).getInput();

        final int method = input.readInt();

        if (method == 0) {
            if (state.getStrategy() != null) {
                throw new PrestoException(
                        CONSTRAINT_VIOLATION,
                        "strategy is not null for null method");
            }
        }
        else if (method == 1) {
            state.setStrategy(new SampleEntropyStateMLEStrategy(input));
        }
        else if (method == 2) {
            state.setStrategy(new SampleEntropyStateJacknifeStrategy(input));
        }
        else {
            throw new InvalidParameterException("unknown method in deserialize");
        }
    }
}
