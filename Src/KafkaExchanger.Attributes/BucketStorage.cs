using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaExchanger
{
    public class BucketStorage
    {
        private readonly int _itemsInBucket;

        private Queue<Bucket> _delayBuckets = new();
        private Bucket _delayBucketsLast;

        private readonly InFly _inFly;

        public BucketStorage(
            int maxBuckets,
            int itemsInBucket,
            Func<int, ValueTask> addNewBucket
            )
        {
            _itemsInBucket = itemsInBucket;
            _inFly = new InFly(
                maxBuckets: maxBuckets,
                itemsInBucket: itemsInBucket,
                addNewBucket: addNewBucket
                );
        }

        public async ValueTask Init(
            int minBuckets,
            Func<ValueTask<int>> currentBucketsCount
            )
        {
            await _inFly.Init(
                minBuckets, 
                currentBucketsCount
                );
        }

        public class PushResult
        {
            public bool NeedStart { get; set; }

            public object Process { get; set; }

            public int BucketId { get; set; }
        }

        public async ValueTask<PushResult> Push(string guid, MessageInfo messageInfo)
        {
            if(_delayBucketsLast != null)
            {
                if (!_delayBucketsLast.HavePlace)
                {
                    _delayBucketsLast = new Bucket(_itemsInBucket);
                    _delayBuckets.Enqueue(_delayBucketsLast);
                }

                _delayBucketsLast.Add(guid, messageInfo);
            }
            else
            {
                var tryAddResult = await _inFly.TryAdd(guid, messageInfo);
                if (tryAddResult.IsSuccess)
                {
                    return new PushResult()
                    {
                        NeedStart = true,
                        Process = messageInfo.TakeProcess(),
                        BucketId = tryAddResult.BucketId
                    };
                }

                _delayBucketsLast = new Bucket(_itemsInBucket);
                _delayBucketsLast.Add(guid, messageInfo);
                _delayBuckets.Enqueue(_delayBucketsLast);
            }

            return new PushResult() 
            {
                NeedStart = false
            };
        }

        public bool TryPop(
            out int bucketId,
            out Dictionary<string, MessageInfo> canFreeInfos,
            out Dictionary<string, MessageInfo> needInitInfos
            )
        {
            var needPop = _delayBuckets.TryPeek(out var addedNewInFly);
            Dictionary<string, MessageInfo> temp = null;
            if(needPop)
            {
                temp = addedNewInFly.Messages;
            }
            var result = _inFly.TryPop(addedNewInFly, out bucketId, out canFreeInfos);

            if(result && needPop)
            {
                needInitInfos = temp;
                _delayBuckets.Dequeue();
                if(_delayBuckets.Count == 0)
                {
                    _delayBucketsLast = null;
                }
            }
            else
            {
                needInitInfos = null;
            }

            return result;
        }

        public void Finish(
            int bucketId,
            string guid,
            Confluent.Kafka.TopicPartitionOffset[] offsets
            )
        {
            var bucket = _inFly.Find(bucketId);
            bucket.Finish(guid, offsets);
        }
    }
}