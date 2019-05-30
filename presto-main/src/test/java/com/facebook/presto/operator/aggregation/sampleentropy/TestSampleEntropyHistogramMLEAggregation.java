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

import com.facebook.presto.operator.aggregation.NumericHistogram;
import com.google.common.collect.Lists;

import java.util.ArrayList;

public class TestSampleEntropyHistogramMLEAggregation
        extends TestSampleEntropyAggregation
{
    public TestSampleEntropyHistogramMLEAggregation()
    {
        super("histogram_mle", null, null);
    }

    @Override
    public Double getExpectedValue(int start, int length)
    {
        final NumericHistogram histogram =
                new NumericHistogram(TestSampleEntropyAggregation.SIZE);
        final ArrayList<Double> samples = new ArrayList<Double>();
        final ArrayList<Double> weights = new ArrayList<Double>();
        super.getSamplesAndWeights(start, length, samples, weights);
        for (int i = 0; i < samples.size(); ++i) {
            histogram.add(samples.get(i), weights.get(i));
        }
        return super.getEntropyFromSamplesAndWeights(
                Lists.newArrayList(histogram.getBuckets().keySet().iterator()),
                Lists.newArrayList(histogram.getBuckets().values().iterator()));
    }
}
