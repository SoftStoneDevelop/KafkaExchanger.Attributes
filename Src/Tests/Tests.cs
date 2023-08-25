using KafkaExchanger;
using NUnit.Framework;
using System;

namespace Tests
{
    public class Tests
    {
        [Test]
        public void Add()
        {
            var array = new HorizonStorage();
            for (long j = 0; j < 5000; j++)
            {
                array.Add(j);
            }

            long i = 0;
            foreach (long item in array)
            {
                Assert.That(item, Is.EqualTo(i));
                i++;
            }
        }

        [Test]
        public void AddNotUnique()
        {
            for (int i = 0; i < 1000; i++)
            {
                var array = new HorizonStorage();
                for (int j = 0; j < 1000; j++)
                {
                    array.Add(j);
                }

                var pass = false;

                try
                {
                    array.Add(i);
                }
                catch(Exception e)
                {
                    pass = true;
                    Assert.That(e.Message, Is.EqualTo("New element already contains, HorizonStorage is corrupted"));
                }

                if(!pass)
                {
                    Assert.Fail($"Not unique value({i}) not throw Exception");
                }
            }
        }

        [Test]
        public void ClearAfter()
        {
            var array = new HorizonStorage();
            for (long j = 0; j < 1000; j++)
            {
                array.Add(j);
            }

            for (int i = 0; i < 1000; i++)
            {
                var indx = array.Find(i);
                array.Clear(indx);
                Assert.That(array.Size, Is.EqualTo(999 - i));
            }

            array.Clear(0);
            Assert.That(array.Size, Is.EqualTo(0));

            array.Clear(0);
            Assert.That(array.Size, Is.EqualTo(0));
        }

        [Test]
        public void Find()
        {
            var items = 1000;
            var array = new HorizonStorage();
            for (long j = 0; j < items; j++)
            {
                array.Add(j);
            }

            var value = items - 1;
            for (long j = 0; j < items; j++)
            {
                var index = array.Find(value--);
                Assert.That(index, Is.EqualTo(j));
            }
        }

        [Test]
        public void CanFree()
        {
            var array = new HorizonStorage();
            for (long j = 0; j < 1000; j++)
            {
                array.Add(j);
            }

            for (long j = 0; j < 1000; j++)
            {
                var index = array.Find(j);
                var canFreeCount = array.CanFree(index);
                Assert.That(canFreeCount, Is.EqualTo(j + 1));
            }
        }
    }
}