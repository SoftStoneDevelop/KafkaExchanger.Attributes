using KafkaExchanger;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tests
{
    [TestFixture]
    [Parallelizable(ParallelScope.All)]
    internal class BucketStorageFixture
    {
        [Test]
        public async Task CanFreeNullOffsets()
        {
            var storage = new BucketStorage(5, 2, 50, static (bucketId) => { return ValueTask.CompletedTask; });
            await storage.Init(static () => { return ValueTask.FromResult(0); });

            var buckets = new List<string[]>();
            string[] currentBucket = null;
            int currentId = 0;
            for (int i = 0; i < 150; i++)//3 buckets
            {
                if (currentId == 0 || currentId == currentBucket.Length)
                {
                    currentBucket = new string[50];
                    buckets.Add(currentBucket);
                    currentId = 0;
                }

                var message = new MessageInfo(2);
                var guid = Guid.NewGuid().ToString("D");
                var bucketId = await storage.Push(guid, message);
                currentBucket[currentId++] = guid;
            }

            var canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(0));

            var bucket = buckets[0];
            for (int j = 0; j < bucket.Length; j++)
            {
                storage.Finish(0, bucket[j]);
            }

            canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(1));

            bucket = buckets[1];
            for (int j = 0; j < bucket.Length; j++)
            {
                storage.Finish(1, bucket[j]);
            }

            canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(2));

            bucket = buckets[2];
            for (int j = 0; j < 20; j++)
            {
                storage.Finish(2, bucket[j]);
            }

            canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(2));

            for (int j = 20; j < bucket.Length; j++)
            {
                storage.Finish(2, bucket[j]);
            }

            canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(3));
        }

        [Test]
        public async Task CanFree()
        {
            var storage = new BucketStorage(5, 2, 50, static (bucketId) => { return ValueTask.CompletedTask; });
            await storage.Init(static () => { return ValueTask.FromResult(0); });

            var buckets = new List<string[]>();
            string[] currentBucket = null;
            int currentId = 0;
            for (int i = 0; i < 150; i++)//3 buckets
            {
                if (currentId == 0 || currentId == currentBucket.Length)
                {
                    currentBucket = new string[50];
                    buckets.Add(currentBucket);
                    currentId = 0;
                }

                var message = new MessageInfo(2);
                var guid = Guid.NewGuid().ToString("D");
                var bucketId = await storage.Push(guid, message);
                currentBucket[currentId++] = guid;
            }

            var canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(0));

            var bucket = buckets[0];
            var bucket0Min = 20;
            var bucket0Max = bucket0Min - 1;
            //20 => 69
            for (int j = 0; j < bucket.Length; j++)
            {
                var offset = ++bucket0Max;
                storage.Finish(0, bucket[j]);
                storage.SetOffset(
                    0,
                    bucket[j],
                    0,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );

                storage.SetOffset(
                    0,
                    bucket[j],
                    1,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );
            }

            canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(1));

            bucket = buckets[1];
            var bucket1Min = 70;
            var bucket1Max = bucket1Min - 1;
            //70 => 119
            for (int j = 0; j < bucket.Length; j++)
            {
                var offset = ++bucket1Max;
                storage.Finish(1, bucket[j]);
                storage.SetOffset(
                    1,
                    bucket[j],
                    0,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );

                storage.SetOffset(
                    1,
                    bucket[j],
                    1,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );
            }

            canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(1));

            bucket = buckets[2];
            var bucket2Min = 0;
            var bucket2Max = bucket2Min - 1;
            //0 => 19
            for (int j = 0; j < 20; j++)
            {
                var offset = ++bucket2Max;
                storage.Finish(2, bucket[j]);
                storage.SetOffset(
                    2,
                    bucket[j],
                    0,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );

                storage.SetOffset(
                    2,
                    bucket[j],
                    1,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );
            }

            canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(0));

            //0-19 + 121 => 171
            bucket2Max = 121;
            for (int j = 20; j < bucket.Length; j++)
            {
                var offset = ++bucket2Max;
                storage.Finish(2, bucket[j]);
                storage.SetOffset(
                    2,
                    bucket[j],
                    0,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );

                storage.SetOffset(
                    2,
                    bucket[j],
                    1,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );
            }

            canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(3));
        }

        [Test]
        public async Task CanStopConsume()
        {
            var storage = new BucketStorage(5, 2, 50, static (bucketId) => { return ValueTask.CompletedTask; });
            await storage.Init(static () => { return ValueTask.FromResult(0); });

            var buckets = new List<string[]>();
            string[] currentBucket = null;
            int currentId = 0;
            for (int i = 0; i < 50 * 7; i++)
            {
                if (currentId == 0 || currentId == currentBucket.Length)
                {
                    currentBucket = new string[50];
                    buckets.Add(currentBucket);
                    currentId = 0;
                }

                var message = new MessageInfo(2);
                var guid = Guid.NewGuid().ToString("D");
                var bucketId = await storage.Push(guid, message);
                currentBucket[currentId++] = guid;
            }

            var canFree = storage.CanFreeBuckets();
            Assert.That(canFree, Has.Count.EqualTo(0));

            var bucketMin = 0;
            var bucketMax = bucketMin - 1;
            for (int i = 0; i < 4; i++)//set offsets in 0..3 buckets
            {
                var bucket = buckets[i];
                for (int j = 0; j < bucket.Length; j++)
                {
                    var offset = ++bucketMax;
                    var canStopConsume = storage.SetOffset(
                        i,
                        bucket[j],
                        0,
                        new Confluent.Kafka.TopicPartitionOffset(
                            new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                            new Confluent.Kafka.Offset(offset)
                            )
                        );
                    Assert.That(canStopConsume, Is.False);
                    canStopConsume = storage.SetOffset(
                        i,
                        bucket[j],
                        1,
                        new Confluent.Kafka.TopicPartitionOffset(
                            new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                            new Confluent.Kafka.Offset(offset)
                            )
                        );
                    Assert.That(canStopConsume, Is.False);
                }
            }

            for (int i = 5; i < 7; i++)//set offsets in 5..6 buckets
            {
                var bucket = buckets[i];
                for (int j = 0; j < bucket.Length; j++)
                {
                    var offset = ++bucketMax;
                    var canStopConsume = storage.SetOffset(
                        i,
                        bucket[j],
                        0,
                        new Confluent.Kafka.TopicPartitionOffset(
                            new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                            new Confluent.Kafka.Offset(offset)
                            )
                        );
                    Assert.That(canStopConsume, Is.False);
                    canStopConsume = storage.SetOffset(
                        i,
                        bucket[j],
                        1,
                        new Confluent.Kafka.TopicPartitionOffset(
                            new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                            new Confluent.Kafka.Offset(offset)
                            )
                        );
                    Assert.That(canStopConsume, Is.False);
                }
            }

            var bucket4 = buckets[4];
            for (int j = 0; j < bucket4.Length; j++)
            {
                var offset = ++bucketMax;
                var canStopConsume = storage.SetOffset(
                    4,
                    bucket4[j],
                    0,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );
                Assert.That(canStopConsume, Is.False);
                canStopConsume = storage.SetOffset(
                    4,
                    bucket4[j],
                    1,
                    new Confluent.Kafka.TopicPartitionOffset(
                        new Confluent.Kafka.TopicPartition("topic1", new Confluent.Kafka.Partition(0)),
                        new Confluent.Kafka.Offset(offset)
                        )
                    );

                if(j == bucket4.Length - 1)
                {
                    Assert.That(canStopConsume, Is.True);
                }
                else
                {
                    Assert.That(canStopConsume, Is.False);
                }
            }
        }

        [Test]
        public async Task PushPopMany()
        {
            var storage = new BucketStorage(5, 2, 10, static (bucketId) => { return ValueTask.CompletedTask; });
            await storage.Init(static () => { return ValueTask.FromResult(0); });

            var guids = new Queue<(string, int)>(1000);
            for (int i = 0; i < 1000; i++)
            {
                for (int j = 0; j < 1000; j++)
                {
                    var guid = Guid.NewGuid().ToString("D");
                    var bucketId = await storage.Push(guid, new MessageInfo(2));
                    storage.Validate();
                    guids.Enqueue((guid, bucketId));
                }

                for (int j = 0; j < guids.Count / 2; j++)
                {
                    var item = guids.Dequeue();
                    storage.Finish(item.Item2, item.Item1);
                    storage.Validate();
                    var free = storage.CanFreeBuckets();
                    foreach (var freeItem in free)
                    {
                        storage.Pop(freeItem);
                        storage.Validate();
                    }
                }

                for (int j = 0; j < 1000; j++)
                {
                    var guid = Guid.NewGuid().ToString("D");
                    var bucketId = await storage.Push(guid, new MessageInfo(2));
                    storage.Validate();
                    guids.Enqueue((guid, bucketId));
                }

                while (guids.TryDequeue(out var item))
                {
                    storage.Finish(item.Item2, item.Item1);
                    storage.Validate();
                    var free = storage.CanFreeBuckets();
                    foreach (var freeItem in free)
                    {
                        storage.Pop(freeItem);
                        storage.Validate();
                    }
                }

                storage.Validate();
            }
        }
    }
}