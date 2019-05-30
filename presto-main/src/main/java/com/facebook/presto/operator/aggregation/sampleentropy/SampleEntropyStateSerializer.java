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
    public static SampleEntropyStateStrategy create(
            long size,
            String method,
            Double arg0,
            Double arg1)
    {
        if (method == null || method.equalsIgnoreCase("histogram_mle")) {
            return new SampleEntropyStateHistogramMLEStrategy(size);
        }

        if (method.equalsIgnoreCase("fixed_histogram_mle")) {
            return new SampleEntropyStateFixedHistogramMLEStrategy(size, arg0, arg1);
        }

        if (method.equalsIgnoreCase("fixed_histogram_jacknife")) {
            return new SampleEntropyStateFixedHistogramJacknifeStrategy(size, arg0, arg1);
        }

        throw new InvalidParameterException(String.format("unknown method %s", method));
    }

    public static void validate(String method, SampleEntropyStateStrategy strategy)
    {
        if (method == null || method.equalsIgnoreCase("histogram_mle")) {
            if (!(strategy instanceof SampleEntropyStateHistogramMLEStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        if (method.equalsIgnoreCase("fixed_histogram_mle")) {
            if (!(strategy instanceof SampleEntropyStateFixedHistogramMLEStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent method");
            }

            return;
        }

        if (method.equalsIgnoreCase("fixed_histogram_jacknife")) {
            if (!(strategy instanceof SampleEntropyStateFixedHistogramJacknifeStrategy)) {
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
        if (target instanceof SampleEntropyStateHistogramMLEStrategy) {
            if (!(source instanceof SampleEntropyStateHistogramMLEStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((SampleEntropyStateHistogramMLEStrategy) target).mergeWith((SampleEntropyStateHistogramMLEStrategy) source);
            return;
        }

        if (target instanceof SampleEntropyStateFixedHistogramMLEStrategy) {
            if (!(source instanceof SampleEntropyStateFixedHistogramMLEStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((SampleEntropyStateFixedHistogramMLEStrategy) target).mergeWith((SampleEntropyStateFixedHistogramMLEStrategy) source);
            return;
        }

        if (target instanceof SampleEntropyStateFixedHistogramJacknifeStrategy) {
            if (!(source instanceof SampleEntropyStateFixedHistogramJacknifeStrategy)) {
                throw new PrestoException(
                        INVALID_FUNCTION_ARGUMENT,
                        "Inconsistent strategy");
            }
            ((SampleEntropyStateFixedHistogramJacknifeStrategy) target).mergeWith((SampleEntropyStateFixedHistogramJacknifeStrategy) source);
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
        else if (strategy instanceof SampleEntropyStateHistogramMLEStrategy) {
            sliceOut.appendInt(1);
        }
        else if (strategy instanceof SampleEntropyStateFixedHistogramMLEStrategy) {
            sliceOut.appendInt(2);
        }
        else if (strategy instanceof SampleEntropyStateFixedHistogramJacknifeStrategy) {
            sliceOut.appendInt(3);
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
            state.setStrategy(new SampleEntropyStateHistogramMLEStrategy(input));
        }
        else if (method == 2) {
            state.setStrategy(new SampleEntropyStateFixedHistogramMLEStrategy(input));
        }
        else if (method == 3) {
            state.setStrategy(new SampleEntropyStateFixedHistogramJacknifeStrategy(input));
        }
        else {
            throw new InvalidParameterException("unknown method in deserialize");
        }
    }
}
