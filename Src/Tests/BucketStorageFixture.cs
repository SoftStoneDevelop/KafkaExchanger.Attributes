using KafkaExchanger;
using NUnit.Framework;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Tests
{
    [TestFixture]
    [Parallelizable(ParallelScope.All)]
    internal class BucketStorageFixture
    {
        [Test]
        public async Task Init()
        {
            var newBuckets = new List<int>();
            var storage = new BucketStorage(
                maxBuckets: 5,
                itemsInBucket: 10, 
                async (newBucketId) => 
                {
                    newBuckets.Add(newBucketId);
                    await Task.CompletedTask;
                }
                );

            await storage.Init(
                minBuckets: 5,
                currentBucketsCount: static async () => 
                {
                    return await Task.FromResult(3);
                }
                );

            Assert.That(newBuckets, Has.Count.EqualTo(2));
            Assert.That(newBuckets[0], Is.EqualTo(3));
            Assert.That(newBuckets[1], Is.EqualTo(4));

            newBuckets.Clear();
            await storage.Init(
                minBuckets: 7,
                currentBucketsCount: static async () =>
                {
                    return await Task.FromResult(3);
                }
                );

            Assert.That(newBuckets, Has.Count.EqualTo(4));
            Assert.That(newBuckets[0], Is.EqualTo(3));
            Assert.That(newBuckets[1], Is.EqualTo(4));
            Assert.That(newBuckets[2], Is.EqualTo(5));
            Assert.That(newBuckets[3], Is.EqualTo(6));

            newBuckets.Clear();
            await storage.Init(
                minBuckets: 5,
                currentBucketsCount: static async () =>
                {
                    return await Task.FromResult(5);
                }
                );

            Assert.That(newBuckets, Has.Count.EqualTo(0));

            newBuckets.Clear();
            await storage.Init(
                minBuckets: 7,
                currentBucketsCount: static async () =>
                {
                    return await Task.FromResult(5);
                }
                );

            Assert.That(newBuckets, Has.Count.EqualTo(2));
            Assert.That(newBuckets[0], Is.EqualTo(5));
            Assert.That(newBuckets[1], Is.EqualTo(6));
        }

        private static PushPopParametrs[] _pushPopCases =
        {
            new PushPopParametrs
            {
                MaxBuckets = 5,
                ItemsInBucket = 10,
                MinBuckets = 2,
                CurrentBuckets = 3
            },

            new PushPopParametrs
            {
                MaxBuckets = 5,
                ItemsInBucket = 10,
                MinBuckets = 5,
                CurrentBuckets = 5
            },

            new PushPopParametrs
            {
                MaxBuckets = 5,
                ItemsInBucket = 10,
                MinBuckets = 1,
                CurrentBuckets = 0
            }
        };

        public class PushPopParametrs
        {
            public int MaxBuckets { get; init; }
            public int ItemsInBucket { get; init; }
            public int MinBuckets { get; init; }
            public int CurrentBuckets { get; init; }
        }

        [Test, TestCaseSource(nameof(_pushPopCases))]
        public async Task PushPop(
            PushPopParametrs parametrs
            )
        {
            var newBuckets = new List<int>();
            var storage = new BucketStorage(
                maxBuckets: parametrs.MaxBuckets,
                itemsInBucket: parametrs.ItemsInBucket,
                async (newBucketId) =>
                {
                    newBuckets.Add(newBucketId);
                    await Task.CompletedTask;
                }
                );

            await storage.Init(
                minBuckets: parametrs.MinBuckets,
                currentBucketsCount: async () =>
                {
                    return await Task.FromResult(parametrs.CurrentBuckets);
                }
                );

            var allCount = parametrs.MaxBuckets + 15;
            var messagePacks = new Queue<MessageInfo[]>();
            for (int i = 0; i < allCount; i++)
            {
                var infos = new MessageInfo[parametrs.ItemsInBucket];
                for (int j = 0; j < parametrs.ItemsInBucket; j++)
                {
                    var message = new MessageInfo();
                    infos[j] = message;
                    await storage.Push(message);

                    Assert.That(message.Id, Is.EqualTo(j));
                }

                messagePacks.Enqueue(infos);
            }

            var expectNewBuckets = parametrs.MaxBuckets - parametrs.CurrentBuckets;
            Assert.That(newBuckets, Has.Count.EqualTo(expectNewBuckets));
            var expectBucketId = parametrs.CurrentBuckets;
            for (int i = 0; i < expectNewBuckets; i++)
            {
                Assert.That(newBuckets[i], Is.EqualTo(expectBucketId++));
            }

            while (messagePacks.Count != 0)
            {
                Assert.That(storage.TryPop(out _, out _, out _), Is.False);

                //finish all Packs in fly
                for (int i = 0; i < parametrs.MaxBuckets; i++)
                {
                    for (int j = 0; j < parametrs.ItemsInBucket; j++)
                    {
                        storage.Finish(
                            bucketId: i,
                            messageId: j,
                            offsets: null
                            );
                    }
                }

                for (int i = 0; i < parametrs.MaxBuckets; i++)
                {
                    Assert.That(storage.TryPop(out _, out var actualMessages, out _), Is.True);
                    var expectMessages = messagePacks.Dequeue();

                    Assert.That(actualMessages, Has.Length.EqualTo(expectMessages.Length));
                    for (int z = 0; z < expectMessages.Length; z++)
                    {
                        Assert.That(ReferenceEquals(expectMessages[z], actualMessages[z]), Is.True);
                    }
                }

                Assert.That(storage.TryPop(out _, out _, out _), Is.False);
            }
        }
    }
}