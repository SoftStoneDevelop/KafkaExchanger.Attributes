using KafkaExchanger;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Tests
{
    [TestFixture]
    [Parallelizable(ParallelScope.All)]
    public class BucketFixture
    {
        [Test]
        public void Add()
        {
            var bucket = new Bucket(maxItems: 100);
            for (long j = 0; j < 100; j++)
            {
                bucket.Add(Guid.NewGuid().ToString(), new MessageInfo());
            }

            var error = Assert.Throws<Exception>(() => bucket.Add(Guid.NewGuid().ToString(), new MessageInfo()));
            Assert.That(error.Message, Is.EqualTo("Item limit exceeded"));
        }

        [Test]
        public void ResetMessages()
        {
            var bucket = new Bucket(maxItems: 100);
            for (long j = 0; j < 100; j++)
            {
                bucket.Add(Guid.NewGuid().ToString(), new MessageInfo());
            }

            Assert.That(bucket.Messages.Count, Is.EqualTo(100));
            var messages = bucket.ResetMessages();
            Assert.That(messages, Is.Not.Null);
            Assert.That(messages, Has.Count.EqualTo(100));
            Assert.That(bucket.Messages.Count, Is.EqualTo(0));
        }

        [Test]
        public void CanFree()
        {
            var storage = new Bucket(maxItems: 100);
            var infos = new List<(string guid, MessageInfo info)>(100);
            for (long j = 0; j < 100; j++)
            {
                var guid = Guid.NewGuid().ToString();
                var info = new MessageInfo();
                storage.Add(guid, info);
                infos.Add((guid,info));
            }

            for (int j = 0; j < infos.Count; j++)
            {
                (string guid, _) = infos[j];
                if(j % 2 == 0)
                {
                    storage.Finish(guid, null);
                }
            }

            Assert.That(storage.CanFree(), Is.False);

            for (int j = 0; j < infos.Count; j++)
            {
                (string guid, _) = infos[j];
                if (j % 2 == 1)
                {
                    storage.Finish(guid, null);
                }

                if(j == infos.Count - 1)
                {
                    Assert.That(storage.CanFree(), Is.True);
                }
                else
                {
                    Assert.That(storage.CanFree(), Is.False);
                }
            }
        }

        [Test]
        public void Finish()
        {
            var storage = new Bucket(maxItems: 100);
            var infos = new List<(string guid, MessageInfo info)>(100);
            for (long j = 0; j < 100; j++)
            {
                var guid = Guid.NewGuid().ToString();
                var info = new MessageInfo();
                storage.Add(guid, info);
                infos.Add((guid, info));
            }

            for (int j = 0; j < infos.Count; j++)
            {
                (string guid, MessageInfo info) = infos[j];
                Assert.That(info.Finished, Is.False);
                storage.Finish(guid, null);
                Assert.That(info.Finished, Is.True);
            }
        }
    }
}