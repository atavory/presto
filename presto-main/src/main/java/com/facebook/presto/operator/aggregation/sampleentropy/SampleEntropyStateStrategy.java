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
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public abstract class SampleEntropyStateStrategy
        implements Cloneable
{
    public void validateParams(long bucketCount, double min, double max, double weight)
    {
        if (weight < 0.0) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Weight must be non-negative");
        }
    }

    public abstract void add(double value, double weight);

    public abstract double calculateEntropy();

    public abstract long estimatedInMemorySize();

    public abstract int getRequiredBytesForSerialization();

    public abstract void serialize(SliceOutput out);

    public abstract void mergeWith(SampleEntropyStateStrategy other);

    @Override
    public abstract SampleEntropyStateStrategy clone();
}
