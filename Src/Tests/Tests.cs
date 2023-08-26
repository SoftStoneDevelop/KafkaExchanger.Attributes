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
                array.Add(new HorizonInfo(j));
            }

            long i = 0;
            foreach (var item in array)
            {
                Assert.That(item.HorizonId, Is.EqualTo(i));
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
                    array.Add(new HorizonInfo(j));
                }

                var pass = false;

                try
                {
                    array.Add(new HorizonInfo(i));
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
        public void ClearFinished()
        {
            var array = new HorizonStorage();
            for (long j = 0; j < 1000; j++)
            {
                array.Add(new HorizonInfo(j));
            }

            Exception ex;
            for (int i = 0; i < 150; i++)
            {
                ex = Assert.Throws<Exception>(array.ClearFinished);
                Assert.That(ex.Message, Is.EqualTo("Nothing to clear"));

                array.Finish(i);
                array.ClearFinished();

                Assert.That(array.Size, Is.EqualTo(999 - i));

                ex = Assert.Throws<Exception>(array.ClearFinished);
                Assert.That(ex.Message, Is.EqualTo("Nothing to clear"));
            }

            int sizeExpect;
            for (int i = 151; i < 1000; i++)
            {
                ex = Assert.Throws<Exception>(array.ClearFinished);
                Assert.That(ex.Message, Is.EqualTo("Nothing to clear"));

                sizeExpect = array.Size;
                array.Finish(i);

                Assert.That(array.Size, Is.EqualTo(sizeExpect));

                ex = Assert.Throws<Exception>(array.ClearFinished);
                Assert.That(ex.Message, Is.EqualTo("Nothing to clear"));
            }

            array.Finish(150);
            array.ClearFinished();

            Assert.That(array.Size, Is.EqualTo(0));
            ex = Assert.Throws<Exception>(array.ClearFinished);
            Assert.That(ex.Message, Is.EqualTo("Nothing to clear"));
        }

        [Test]
        public void Find()
        {
            var items = 1000;
            var array = new HorizonStorage();
            for (long j = 0; j < items; j++)
            {
                array.Add(new HorizonInfo(j));
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
                array.Add(new HorizonInfo(j));
            }

            for (long j = 0; j < 1000; j++)
            {
                if(j % 2 == 0)
                {
                    array.Finish(j);
                }
            }

            var canFree = 1;
            for (long j = 0; j < 1000; j++)
            {
                if (j % 2 == 1)
                {
                    Assert.That(array.CanFree(), Is.EqualTo(canFree));
                    array.Finish(j);
                    if(j == 999)
                    {
                        canFree += 1;
                    }
                    else
                    {
                        canFree += 2;
                    }
                    Assert.That(array.CanFree(), Is.EqualTo(canFree));
                }
                else
                {
                    Assert.That(array.CanFree(), Is.EqualTo(canFree));
                }
            }
        }

        [Test]
        public void AfterHorizon()
        {
            var array = new HorizonStorage();
            for (long j = 0; j < 1000; j++)
            {
                array.Add(new HorizonInfo(j));
            }

            Assert.That(array.AfterHorizon(), Is.Null);
            for (long j = 0; j < 1000; j++)
            {
                if (j % 2 == 0)
                {
                    array.Finish(j);
                }
            }

            for (int j = 0; j < 1000; j++)
            {
                if (j % 2 == 1)
                {
                    array.Finish(j);
                    if(j == 999)
                    {
                        Assert.That(array.AfterHorizon(), Is.EqualTo(array[0]));
                    }
                    else
                    {
                        Assert.That(array.AfterHorizon(), Is.EqualTo(array[(999 - j) - 1]));
                    }
                }
                else
                {
                    Assert.That(array.AfterHorizon(), Is.EqualTo(array[999 - j]));
                }
            }
        }

        [Test]
        public void Finish()
        {
            var array = new HorizonStorage();
            for (long j = 0; j < 1000; j++)
            {
                array.Add(new HorizonInfo(j));
            }

            Assert.That(array.AfterHorizon(), Is.Null);
            for (int j = 0; j < 1000; j++)
            {
                var finished = array.Finish(j);
                Assert.That(finished, Is.EqualTo(array[999 - j]));
            }
        }
    }
}