using KafkaExchanger;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
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
            var messagePacks = new Queue<(int bucketId, string guid, MessageInfo info)[]>();
            for (int i = 0; i < allCount; i++)
            {
                var infos = new (int bucketId, string guid, MessageInfo info)[parametrs.ItemsInBucket];
                for (int j = 0; j < parametrs.ItemsInBucket; j++)
                {
                    var guid = Guid.NewGuid().ToString();
                    var message = new MessageInfo();
                    var result = await storage.Push(guid, message);
                    infos[j] = (result.BucketId, guid, message);
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

            var iteration = 0;
            var needInitBucketId = new Queue<int>();
            while (messagePacks.Count != 0)
            {
                Assert.That(storage.TryPop(out _, out _, out _), Is.False);
                var expectMessages = messagePacks.Dequeue();
                if (++iteration > parametrs.MaxBuckets)
                {
                    var needInit = needInitBucketId.Dequeue();
                    for (int i = 0; i < expectMessages.Length; i++)
                    {
                        expectMessages[i].bucketId = needInit;
                    }
                }

                for (int i = 0; i < expectMessages.Length; i++)
                {
                    var (bucketId, guid, info) = expectMessages[i];
                    storage.Finish(
                        bucketId: bucketId,
                        guid: guid,
                        offsets: null
                        );
                }

                Assert.That(storage.TryPop(out var actualBucketId, out var actualMessages, out var needInitInfos), Is.True);

                Assert.That(actualMessages, Has.Count.EqualTo(expectMessages.Length));
                for (int i = 0; i < expectMessages.Length; i++)
                {
                    var (bucketId, guid, info) = expectMessages[i];
                    var actual = actualMessages[guid];

                    Assert.That(actualBucketId, Is.EqualTo(bucketId));
                    Assert.That(ReferenceEquals(info, actual), Is.True);
                }

                if(needInitInfos != null)
                {
                    needInitBucketId.Enqueue(actualBucketId);
                }

                Assert.That(storage.TryPop(out _, out _, out _), Is.False);
            }
        }
    }
}