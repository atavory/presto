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
package com.facebook.presto.operator.aggregation.differentialentropy;

import com.facebook.presto.operator.aggregation.fixedhistogram.FixedHistogram;
import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/*
Abstract base class for different strategies for calculating entropy based on fixed histograms.
 */
public abstract class DifferentialEntropyStateFixedHistogramStrategy
        extends DifferentialEntropyStateStrategy
{
    protected final FixedHistogram histogram;

    public DifferentialEntropyStateFixedHistogramStrategy(FixedHistogram histogram)
    {
        this.histogram = histogram;
    }

    @Override
    public void validateParams(
            long bucketCount,
            double sample,
            Double weight,
            Double min,
            Double max)
    {
        super.validateParams(bucketCount, sample, weight, min, max);

        if (histogram.getBucketCount() != bucketCount) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Inconsistent bucket count");
        }
        if (histogram.getMin() != min) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Inconsistent min");
        }
        if (histogram.getMax() != max) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Inconsistent max");
        }
        if (sample < min) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Sample must be at least min");
        }
        if (sample > max) {
            throw new PrestoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Sample must be at most max");
        }
    }
}
