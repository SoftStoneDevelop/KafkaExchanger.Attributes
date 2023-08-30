using KafkaExchanger;
using NUnit.Framework;
using System.Collections.Generic;

namespace Tests
{
    [TestFixture]
    [Parallelizable(ParallelScope.All)]
    internal class InFlyViewFixture
    {
        private static int[][] _addCases = 
        {
            //Tail after head
            // |0|H|*|T|4|
            new int[]{ 1, 2, 3 },
            // |H|*|*|*|T|
            new int[]{ 0, 1, 2, 3, 4 },
            // |H|T|2|3|4|
            new int[]{ 0, 1 },
            // |0|1|2|H|T|
            new int[]{ 3, 4 },

            //Head after tail
            // |*|T|2|H|*|
            new int[]{ 3, 4, 0, 1 },
            // |T|1|2|3|H|
            new int[]{ 4, 0 },
            // |T|1|2|H|*|
            new int[]{ 3, 4, 0 },
            // |*|*|*|T|H|
            new int[]{ 4, 0, 1, 2, 3 }
        };

        [Test, TestCaseSource(nameof(_addCases))]
        public void Add(int[] fullBucket)
        {
            var buckets = new Bucket[5];
            for (int i = 0; i < buckets.Length; i++)
            {
                buckets[i] = new Bucket(10);
            }

            var view = new InFlyView(
                buckets: buckets,
                head: fullBucket[0],
                tail: fullBucket[^1],
                current: fullBucket[0],
                size: fullBucket.Length
                );

            var fullCurrent = new List<int>() { };
            for (int indx = 0; indx < fullBucket.Length; indx++)
            {
                var full = fullBucket[indx];
                for (int i = 0; i < 10; i++)
                {
                    Assert.That(view.TryAdd(new MessageInfo()), Is.True);
                }

                for (int i = 0; i < buckets.Length; i++)
                {
                    if (fullCurrent.Contains(i) || i == full)
                    {
                        Assert.That(buckets[i].HavePlace, Is.False);
                    }
                    else
                    {
                        Assert.That(buckets[i].Size, Is.EqualTo(0));
                    }
                }

                Assert.That(view.Size, Is.EqualTo(fullBucket.Length));
                Assert.That(view.GlobalHeadIndex, Is.EqualTo(fullBucket[0]));
                Assert.That(view.GlobalTailIndex, Is.EqualTo(fullBucket[^1]));
                fullCurrent.Add(full);
            }
        }
    }
}