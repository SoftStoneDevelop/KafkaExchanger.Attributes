using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace KafkaExchanger
{
    public class BucketStorage
    {
        private readonly Func<int, ValueTask> _addNewBucket;
        private readonly int _itemsInBucket;
        private readonly int _inputs;

        private int _current;
        private int _head;

        private Bucket[] _buckets;

        public BucketStorage(
            int inputs,
            int itemsInBucket,
            Func<int, ValueTask> addNewBucket
            )
        {
            _inputs = inputs;
            _addNewBucket = addNewBucket;
            _itemsInBucket = Math.Max(itemsInBucket, 1);
        }

        public async ValueTask Init(
            Func<ValueTask<int>> currentBucketsCount
            )
        {
            var currentBuckets = await currentBucketsCount();
            var size = Math.Max(1, currentBuckets);
            _buckets = new Bucket[size];
            for (int i = 0; i < size; i++)
            {
                var bucket = new Bucket(maxItems: _itemsInBucket, offsetSize: _inputs)
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

        private async ValueTask Expand()
        {
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

            var bucket = new Bucket(maxItems: _itemsInBucket, offsetSize: _inputs)
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
        }

        public async ValueTask<int> Push(string guid, MessageInfo messageInfo)
        {
            if (_buckets[_current].HavePlace)
            {
                _buckets[_current].Add(guid, messageInfo);
                return _buckets[_current].BucketId;
            }

            if (TryMoveNext())
            {
                _buckets[_current].Add(guid, messageInfo);
                return _buckets[_current].BucketId;
            }

            await Expand();

            TryMoveNext();
            _buckets[_current].Add(guid, messageInfo);
            return _buckets[_current].BucketId;
        }

        public void Pop(Bucket bucket)
        {
            var head = _buckets[_head];
            if(!ReferenceEquals(head, bucket))
            {
                throw new InvalidOperationException();
            }

            head.Reset();
            if (++_head == _buckets.Length)
            {
                _head = 0;
            }
        }

        public void SetOffset(
            int bucketId,
            string guid,
            int offsetId,
            Confluent.Kafka.TopicPartitionOffset offset
            )
        {
            for (int i = 0; i < _buckets.Length; i++)
            {
                var bucket = _buckets[i];
                if(bucket.BucketId == bucketId)
                {
                    bucket.SetOffset(guid, offsetId, offset);   
                }
            }

            throw new InvalidOperationException();
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

        public List<Bucket> CanFreeBuckets()
        {
            var result = new List<Bucket>();
            var current = _buckets[_head];
            if(!current.CanFree())
            {
                return result;
            }

            result.Add(current);

            var scopeMax = current.MaxOffset;
            var currentId = _head++;
            if(currentId == _buckets.Length)
            {
                currentId = 0;
            }
            
            current = _buckets[currentId];
            if (current.IsEmpty())
            {
                return result;
            }

            while (current.CanFree() && currentId != _head)
            {
                var minOffsets = current.MinOffset;
                var canFree = false;
                for (int j = 0; j < scopeMax.Length; j++)
                {
                    canFree &= minOffsets[j] > scopeMax[j];
                }

                result.Add(current);
                if (canFree)
                {
                    return result;
                }
                else
                {
                    scopeMax = current.MaxOffset;
                }

                if (++currentId == _buckets.Length)
                {
                    currentId = 0;
                }
                current = _buckets[currentId];
            }

            result.Clear();
            return result;
        }
    }
}