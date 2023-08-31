using System;
using System.Threading.Tasks;

namespace KafkaExchanger
{
    public class InFly
    {
        private readonly Func<int, ValueTask> _addNewBucket;
        private readonly int _itemsInBucket;
        private readonly int _maxBuckets;

        private int _current;
        private int _head;

        private Bucket[] _buckets;

        public InFly(
            int maxBuckets,
            int itemsInBucket,
            Func<int, ValueTask> addNewBucket
            )
        {
            _addNewBucket = addNewBucket;
            _itemsInBucket = Math.Max(itemsInBucket, 1);
            _maxBuckets = Math.Max(maxBuckets, 1);
        }

        public async ValueTask Init(
            int minBuckets,
            Func<ValueTask<int>> currentBucketsCount
            )
        {
            var currentBuckets = await currentBucketsCount();
            var size = Math.Max(Math.Max(minBuckets, 1), currentBuckets);
            _buckets = new Bucket[size];
            for (int i = 0; i < size; i++)
            {
                var bucket = new Bucket(_itemsInBucket)
                {
                    BucketId = i
                };

                _buckets[i] = bucket;

                if (i >= currentBuckets)
                {
                    await _addNewBucket(bucket.BucketId);
                }
            }

            _head = 0;
        }

        private async ValueTask<bool> TryExpand()
        {
            if(_buckets.Length >= _maxBuckets)
            {
                return false;
            }

            var newBuckets = new Bucket[_buckets.Length + 1];
            var initSize = newBuckets.Length - _buckets.Length;

            var toCurrentSize = _current + 1;
            Array.Copy(
                sourceArray: _buckets,
                sourceIndex: 0,
                destinationArray: newBuckets,
                destinationIndex: 0,
                length: toCurrentSize
                );

            var bucket = new Bucket(_itemsInBucket)
            {
                BucketId = _buckets.Length
            };

            newBuckets[_current + 1] = bucket;
            await _addNewBucket(bucket.BucketId);

            var toEndSize = _buckets.Length - toCurrentSize;
            if(toEndSize != 0)
            {
                Array.Copy(
                    sourceArray: _buckets,
                    sourceIndex: _current + 2,
                    destinationArray: newBuckets,
                    destinationIndex: _buckets.Length - toEndSize,
                    length: toEndSize
                    );
            }
            
            _buckets = newBuckets;
            return true;
        }

        public class AddResult
        {
            public bool IsSuccess { get; set; }

            public int BucketId { get; set; }
        }

        public async ValueTask<AddResult> TryAdd(MessageInfo messageInfo)
        {
            if (_buckets[_current].HavePlace)
            {
                _buckets[_current].Add(messageInfo);
                return new AddResult
                {
                    IsSuccess = true,
                    BucketId = _buckets[_current].BucketId
                };
            }

            if (TryMoveNext())
            {
                _buckets[_current].Add(messageInfo);
                return new AddResult
                {
                    IsSuccess = true,
                    BucketId = _buckets[_current].BucketId
                };
            }

            if (!await TryExpand())
            {
                return new AddResult
                {
                    IsSuccess = false
                };
            }

            TryMoveNext();
            _buckets[_current].Add(messageInfo);
            return new AddResult
            {
                IsSuccess = true,
                BucketId = _buckets[_current].BucketId
            };
        }

        public bool TryPop(
            Bucket delayBucket,
            out int freeBucketId,
            out MessageInfo[] freeInfos
            )
        {
            var head = _buckets[_head];
            if (!head.CanFree())
            {
                freeBucketId = -1;
                freeInfos = null;
                return false;
            }

            freeBucketId = head.BucketId;
            if (delayBucket == null)
            {
                freeInfos = head.ResetMessages();
            }
            else
            {
                freeInfos = head.SetMessages(delayBucket);
            }

            if (++_head == _buckets.Length)
            {
                _head = 0;
            }

            return true;
        }

        private bool TryMoveNext()
        {
            var nextIndex = _current + 1;
            if(nextIndex >= _buckets.Length)
            {
                nextIndex = 0;
            }

            if (_buckets[nextIndex].HavePlace)
            {
                _current = nextIndex;
                return true;
            }
            else
            {
                return false;
            }
        }

        public Bucket Find(int bucketId)
        {
            for (int i = 0; i < _buckets.Length; i++)
            {
                var bucket = _buckets[i];
                if (bucket.BucketId == bucketId)
                {
                    return bucket;
                }
            }

            throw new Exception("Bucket not found");
        }
    }
}