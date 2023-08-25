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
            var array = new UniqueSortedArray();
            for (long j = 0; j < 5000; j++)
            {
                array.Add(j);
            }

            long i = 0;
            foreach (ref long item in array)
            {
                Assert.That(item, Is.EqualTo(i));
                i++;
            }
        }

        [Test]
        public void AddNotUnique()
        {
            for (int i = 0; i < 5000; i++)
            {
                var array = new UniqueSortedArray();
                for (int j = 0; j < 5000; j++)
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
                    Assert.That(e.Message, Is.EqualTo("New element already contains in array, array is corrupted"));
                }

                if(!pass)
                {
                    Assert.Fail($"Not unique value({i}) not throw Exception");
                }
            }
        }
    }
}