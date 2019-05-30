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
package com.facebook.presto.operator.aggregation.fixedhistogram;

import com.google.common.collect.Streams;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestFixedDoubleBreakdownHistogram
{
    @Test
    public void getters()
    {
        final FixedDoubleBreakdownHistogram histogram = new FixedDoubleBreakdownHistogram(200, 3.0, 4.0);

        assertEquals(histogram.getBucketCount(), 200);
        assertEquals(histogram.getMin(), 3.0);
        assertEquals(histogram.getMax(), 4.0);
    }

    @Test
    public void illegalBucketCount()
    {
        try {
            new FixedDoubleBreakdownHistogram(-200, 3.0, 4.0);
            fail("exception expected");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("bucketCount"));
        }
    }

    @Test
    public void illegalMinMax()
    {
        try {
            new FixedDoubleBreakdownHistogram(-200, 3.0, 3.0);
            fail("exception expected");
        }
        catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("bucketCount"));
        }
    }

    @Test
    public void basicOps()
    {
        final FixedDoubleBreakdownHistogram histogram = new FixedDoubleBreakdownHistogram(200, 3.0, 4.0);

        histogram.add(3.1, 100.0);
        histogram.add(3.8, 200.0);
        histogram.add(3.1, 100.0);
        assertEquals(
                Streams.stream(histogram.iterator())
                        .mapToDouble(
                                c ->
                                        c.breakdown.entrySet().stream()
                                                .mapToDouble(e -> e.getKey() * e.getValue())
                                                .sum())
                        .sum(),
                400.0);
    }
}
