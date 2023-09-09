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
            //20 => 60
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
            var bucket1Min = 61;
            var bucket1Max = bucket1Min - 1;
            //61 => 111
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
                var offset = ++bucket0Max;
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

            bucket2Max = 111;
            for (int j = 20; j < bucket.Length; j++)
            {
                var offset = ++bucket0Max;
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
    }
}