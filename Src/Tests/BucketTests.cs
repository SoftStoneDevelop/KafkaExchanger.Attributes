using KafkaExchanger;
using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Tests
{
    public class BucketTests
    {
        [Test]
        public void Add()
        {
            var storage = new Bucket(maxItems: 100);
            for (long j = 0; j < 100; j++)
            {
                storage.Add(new MessageInfo());
            }

            long i = 0;
            foreach (var info in storage)
            {
                Assert.That(info.Id, Is.EqualTo(i++));
            }

            var error = Assert.Throws<Exception>(() => storage.Add(new MessageInfo()));
            Assert.That(error.Message, Is.EqualTo("Item limit exceeded"));
        }

        [Test]
        public void Clear()
        {
            var storage = new Bucket(maxItems: 100);
            for (long j = 0; j < 100; j++)
            {
                storage.Add(new MessageInfo());
            }

            Assert.That(storage.Size, Is.EqualTo(100));
            storage.Clear();
            Assert.That(storage.Size, Is.EqualTo(0));
        }

        [Test]
        public void Find()
        {
            var storage = new Bucket(maxItems: 100);
            var infos = new List<MessageInfo>(100);
            for (long j = 0; j < 100; j++)
            {
                var info = new MessageInfo();
                storage.Add(info);
                infos.Add(info);
            }

            for (long j = 0; j < 100; j++)
            {
                var index = storage.Find(j);
                Assert.That(index, Is.EqualTo(j));
                Assert.That(infos[index].Id, Is.EqualTo(j));
            }
        }

        [Test]
        public void CanFree()
        {
            var storage = new Bucket(maxItems: 100);
            var infos = new List<MessageInfo>(100);
            for (long j = 0; j < 100; j++)
            {
                var info = new MessageInfo();
                storage.Add(info);
                infos.Add(info);
            }

            for (int j = 0; j < infos.Count; j++)
            {
                var info = infos[j];
                if(j % 2 == 0)
                {
                    Assert.That(info.Finished, Is.False);
                    storage.Finish(info.Id, null);
                    Assert.That(info.Finished, Is.True);
                }
            }

            Assert.That(storage.CanFree(), Is.False);

            for (int j = 0; j < infos.Count; j++)
            {
                var info = infos[j];
                if (j % 2 == 1)
                {
                    Assert.That(info.Finished, Is.False);
                    storage.Finish(info.Id, null);
                    Assert.That(info.Finished, Is.True);
                }
                else
                {
                    Assert.That(info.Finished, Is.True);
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
            var infos = new List<MessageInfo>(100);
            for (long j = 0; j < 100; j++)
            {
                var info = new MessageInfo();
                storage.Add(info);
                infos.Add(info);
            }

            for (int j = 0; j < infos.Count; j++)
            {
                var info = infos[j];
                Assert.That(info.Finished, Is.False);
                storage.Finish(info.Id, null);
                Assert.That(info.Finished, Is.True);
            }
        }
    }
}