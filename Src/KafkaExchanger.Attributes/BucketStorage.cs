using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static System.Formats.Asn1.AsnWriter;

namespace KafkaExchanger
{
    public class BucketStorage
    {
        private readonly Func<int, ValueTask> _addNewBucket;
        private readonly int _itemsInBucket;
        private readonly int _inputs;
        private readonly int _inFlyLimit;

        private int _current;
        private int _head;
        private int _inUse;

        private Bucket[] _buckets;

        public BucketStorage(
            int inFlyLimit,
            int inputs,
            int itemsInBucket,
            Func<int, ValueTask> addNewBucket
            )
        {
            _inFlyLimit = inFlyLimit;
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
            var bucket = new Bucket(maxItems: _itemsInBucket, offsetSize: _inputs)
            {
                BucketId = _buckets.Length
            };

            if (_current == _buckets.Length - 1)
            {
                newBuckets[0] = bucket;
                await _addNewBucket(bucket.BucketId);

                Array.Copy(
                    sourceArray: _buckets,
                    sourceIndex: 0,
                    destinationArray: newBuckets,
                    destinationIndex: 1,
                    length: _buckets.Length
                    );
                _current = 0;
            }
            else
            {
                var copyBeforeSize = _current + 1;
                Array.Copy(
                    sourceArray: _buckets,
                    sourceIndex: 0,
                    destinationArray: newBuckets,
                    destinationIndex: 0,
                    length: copyBeforeSize
                    );

                newBuckets[_current + 1] = bucket;
                await _addNewBucket(bucket.BucketId).ConfigureAwait(false);

                Array.Copy(
                    sourceArray: _buckets,
                    sourceIndex: copyBeforeSize,
                    destinationArray: newBuckets,
                    destinationIndex: copyBeforeSize + 1,
                    length: _buckets.Length - copyBeforeSize
                    );

                _current++;
            }

            _buckets = newBuckets;
        }

        public async ValueTask<int> Push(string guid, MessageInfo messageInfo)
        {
            if (_buckets[_current].HavePlace)
            {
                if (_inUse == 0)
                {
                    _inUse++;
                }
                _buckets[_current].Add(guid, messageInfo);
                return _buckets[_current].BucketId;
            }

            if (TryMoveNext())
            {
                _buckets[_current].Add(guid, messageInfo);
                _inUse++;
                return _buckets[_current].BucketId;
            }

            await Expand();
            _buckets[_current].Add(guid, messageInfo);

            _inUse++;
            return _buckets[_current].BucketId;
        }

        public void Push(
            int bucketId,
            string guid,
            MessageInfo messageInfo
            )
        {
            var bucket = Find(bucketId);
            bucket.Add(guid, messageInfo);
        }

        public void Validate()
        {
            var endFind = false;
            for (int i = 0; i < _buckets.Length; i++)
            {
                var current = _buckets[_current];
                if (endFind && !current.IsEmpty())
                {
                    throw new Exception("Storage fragmented");
                }

                if (!current.IsFull() || i == _buckets.Length - 1)
                {
                    endFind = true;
                    continue;
                }
            }
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
            _inUse--;
        }

        public bool SetOffset(
            int bucketId,
            string guid,
            int offsetId,
            Confluent.Kafka.TopicPartitionOffset offset
            )
        {
            var bucket = Find(bucketId);
            bucket.SetOffset(guid, offsetId, offset);
            if (_inUse > _inFlyLimit)
            {
                return OnlyWait();
            }

            return false;
        }

        private Bucket Find(int bucketId)
        {
            for (int i = 0; i < _buckets.Length; i++)
            {
                var bucket = _buckets[i];
                if (bucket.BucketId == bucketId)
                {
                    return bucket;
                }
            }

            throw new InvalidOperationException("Bucket not found");
        }

        public void Finish(
            int bucketId,
            string guid
            )
        {
            var bucket = Find(bucketId);
            bucket.Finish(guid);
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
            var mustBeFreeSequence = new List<Bucket>();
            var current = _buckets[_head];
            if(!current.CanFree())
            {
                return mustBeFreeSequence;
            }

            mustBeFreeSequence.Add(current);

            var scopeMax = new TopicPartitionOffset[current.MaxOffset.Length];
            current.MaxOffset.CopyTo(scopeMax, 0);

            var currentId = _head + 1;
            if(currentId == _buckets.Length)
            {
                currentId = 0;
            }
            
            current = _buckets[currentId];
            if (currentId == _head || current.IsEmpty())
            {
                return mustBeFreeSequence;
            }

            var potentiallyInScope = new List<Bucket>();
            Confluent.Kafka.TopicPartitionOffset[] maxPIS = null;
            while (!current.IsEmpty() && currentId != _head)
            {
                var inScope = true;
                for (int j = 0; j < scopeMax.Length; j++)
                {
                    inScope &= scopeMax[j] != null && current.MinOffset[j] != null && (current.MinOffset[j].Offset.Value < scopeMax[j].Offset.Value);
                }

                if (inScope)
                {
                    if(!current.CanFree())
                    {
                        return new List<Bucket>(0);
                    }

                    if(potentiallyInScope.Count != 0)
                    {
                        if (potentiallyInScope.Any(an => !an.CanFree()))
                        {
                            return new List<Bucket>(0);
                        }

                        for (int j = 0; j < potentiallyInScope.Count; j++)
                        {
                            mustBeFreeSequence.Add(potentiallyInScope[j]);
                        }
                        potentiallyInScope.Clear();

                        for (int j = 0; j < scopeMax.Length; j++)
                        {
                            if (maxPIS[j] != null && (scopeMax[j] == null || maxPIS[j].Offset.Value > scopeMax[j].Offset.Value))
                            {
                                scopeMax[j] = maxPIS[j];
                            }
                        }

                        maxPIS = null;
                    }

                    mustBeFreeSequence.Add(current);
                    for (int j = 0; j < scopeMax.Length; j++)
                    {
                        if (current.MaxOffset[j]!= null && (scopeMax[j] == null || current.MaxOffset[j].Offset.Value > scopeMax[j].Offset.Value))
                        {
                            scopeMax[j] = current.MaxOffset[j];
                        }
                    }
                }
                else
                {
                    potentiallyInScope.Add(current);
                    if(maxPIS == null)
                    {
                        maxPIS = new TopicPartitionOffset[current.MaxOffset.Length];
                        current.MaxOffset.CopyTo(maxPIS, 0);
                    }
                    else
                    {
                        for (int j = 0; j < maxPIS.Length; j++)
                        {
                            if (current.MaxOffset[j] != null && (maxPIS[j] == null || maxPIS[j].Offset.Value < current.MaxOffset[j].Offset.Value))
                            {
                                maxPIS[j] = current.MaxOffset[j];
                            }
                        }
                    }
                }

                if (++currentId == _buckets.Length)
                {
                    currentId = 0;
                }
                current = _buckets[currentId];
            }

            foreach (var pis in potentiallyInScope)
            {
                var isNull = true;
                if(!pis.CanFree())
                {
                    break;
                }

                for (int i = 0; i < pis.MaxOffset.Length; i++)
                {
                    isNull &= pis.MaxOffset[i] == null;
                }

                if(!isNull)
                {
                    break;
                }

                mustBeFreeSequence.Add(pis);
            }

            return mustBeFreeSequence.Any(an => !an.CanFree()) ? new List<Bucket>(0) : mustBeFreeSequence;
        }

        public bool OnlyWait()
        {
            var onlyWaitFinish = new List<Bucket>();
            var current = _buckets[_head];
            if (!current.OnlyWaitFinish())
            {
                return false;
            }

            onlyWaitFinish.Add(current);

            var scopeMax = new TopicPartitionOffset[current.MaxOffset.Length];
            current.MaxOffset.CopyTo(scopeMax, 0);

            var currentId = _head + 1;
            if (currentId == _buckets.Length)
            {
                currentId = 0;
            }

            current = _buckets[currentId];
            if (currentId == _head || current.IsEmpty())
            {
                return true;
            }

            var potentiallyInScope = new List<Bucket>();
            Confluent.Kafka.TopicPartitionOffset[] maxPIS = null;
            while (!current.IsEmpty() && currentId != _head)
            {
                var inScope = true;
                for (int j = 0; j < scopeMax.Length; j++)
                {
                    inScope &= scopeMax[j] != null && current.MinOffset[j] != null && (current.MinOffset[j].Offset.Value < scopeMax[j].Offset.Value);
                }

                if (inScope)
                {
                    if (!current.OnlyWaitFinish())
                    {
                        return false;
                    }

                    if (potentiallyInScope.Count != 0)
                    {
                        if (potentiallyInScope.Any(an => !an.OnlyWaitFinish()))
                        {
                            return false;
                        }

                        for (int j = 0; j < potentiallyInScope.Count; j++)
                        {
                            onlyWaitFinish.Add(potentiallyInScope[j]);
                        }
                        potentiallyInScope.Clear();

                        for (int j = 0; j < scopeMax.Length; j++)
                        {
                            if (maxPIS[j] != null && (scopeMax[j] == null || maxPIS[j].Offset.Value > scopeMax[j].Offset.Value))
                            {
                                scopeMax[j] = maxPIS[j];
                            }
                        }

                        maxPIS = null;
                    }

                    onlyWaitFinish.Add(current);
                    for (int j = 0; j < scopeMax.Length; j++)
                    {
                        if (current.MaxOffset[j] != null && (scopeMax[j] == null || current.MaxOffset[j].Offset.Value > scopeMax[j].Offset.Value))
                        {
                            scopeMax[j] = current.MaxOffset[j];
                        }
                    }
                }
                else
                {
                    potentiallyInScope.Add(current);
                    if (maxPIS == null)
                    {
                        maxPIS = new TopicPartitionOffset[current.MaxOffset.Length];
                        current.MaxOffset.CopyTo(maxPIS, 0);
                    }
                    else
                    {
                        for (int j = 0; j < maxPIS.Length; j++)
                        {
                            if (current.MaxOffset[j] != null && (maxPIS[j] == null || maxPIS[j].Offset.Value < current.MaxOffset[j].Offset.Value))
                            {
                                maxPIS[j] = current.MaxOffset[j];
                            }
                        }
                    }
                }

                if (++currentId == _buckets.Length)
                {
                    currentId = 0;
                }
                current = _buckets[currentId];
            }

            foreach (var pis in potentiallyInScope)
            {
                if (!pis.OnlyWaitFinish())
                {
                    break;
                }

                onlyWaitFinish.Add(pis);
            }

            return onlyWaitFinish.Any(an => !an.OnlyWaitFinish()) ? false : onlyWaitFinish.Count >= _inFlyLimit;
        }
    }
}