using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace KafkaExchanger
{
    public class BucketStorage
    {
        private static int NotHaveAfterView = -1;

        private readonly Func<int, ValueTask> _addNewBucket;
        private readonly int _itemsInBucket;

        private int _maxBuckets;
        private InFlyView _view;

        private Bucket[] _buckets;
        private int _afterViewCurrent = NotHaveAfterView;
        private int _size = 0;

        public BucketStorage(
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
                _buckets[i] = new Bucket(_itemsInBucket)
                {
                    BucketId = i
                };

                if (i >= currentBuckets)
                {
                    await _addNewBucket(_buckets[i].BucketId);
                }
            }

            _view = new InFlyView(
                buckets: _buckets,
                head: 0,
                tail: 0,
                current: 0,
                size: size
                );
            _size = size;
        }

        private void ExpandCapacity()
        {
            var newBuckets = new Bucket[_buckets.Length * 2];
            int tail = _afterViewCurrent == NotHaveAfterView ? _view.GlobalTailIndex : _afterViewCurrent;
            if (tail > _view.GlobalHeadIndex)
            {
                Array.Copy(
                    sourceArray: _buckets,
                    sourceIndex: _view.GlobalHeadIndex,
                    destinationArray: newBuckets,
                    destinationIndex: 0,
                    length: _size
                    );
            }
            else
            {
                var headToEndCount = _buckets.Length - _view.GlobalHeadIndex;
                Array.Copy(
                    sourceArray: _buckets,
                    sourceIndex: _view.GlobalHeadIndex,
                    destinationArray: newBuckets,
                    destinationIndex: 0,
                    length: headToEndCount
                    );

                Array.Copy(
                    sourceArray: _buckets,
                    sourceIndex: 0,
                    destinationArray: newBuckets,
                    destinationIndex: headToEndCount,
                    length: tail + 1
                    );
            }

            _view = new InFlyView(
                    buckets: _buckets,
                    head: 0,
                    tail: _view.Size - 1,
                    current: _view.CurrentIndexLocal(),
                    size: _view.Size
                    );

            for (int i = _buckets.Length; i < newBuckets.Length; i++)
            {
                _buckets[i] = new Bucket(_itemsInBucket);
            }

            _buckets = newBuckets;
            if(_afterViewCurrent != NotHaveAfterView)
            {
                _afterViewCurrent = _view.GlobalTailIndex + 1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private async ValueTask<bool> TryAddInView(MessageInfo messageInfo)
        {
            if (_afterViewCurrent != NotHaveAfterView)
            {
                return false;
            }

            if (_view.TryAdd(messageInfo))
            {
                return true;
            }

            if (_view.Size >= _maxBuckets)
            {
                return false;
            }

            ExpandCapacity();
            // |H|=>|T||Size|
            var oldView = _view;
            _view = new InFlyView(
                buckets: _buckets,
                head: _view.GlobalHeadIndex,
                tail: _view.GlobalTailIndex,
                current: _view.GlobalCurrentIndex,
                size: _view.Size + 1
                );
            _size = _view.Size;

            // |H|=>|T||*||Size|
            _buckets[_view.GlobalTailIndex + 1].BucketId = _view.GlobalTailIndex + 1;
            await _addNewBucket(_view.GlobalTailIndex + 1);

            if (_view.TryAdd(messageInfo))
            {
                return true;
            }

            throw new InvalidOperationException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AddAfterView(MessageInfo messageInfo)
        {
            if (_buckets[_afterViewCurrent].HavePlace)
            {
                _buckets[_afterViewCurrent].Add(messageInfo);
                return;
            }

            if(_size == _buckets.Length)
            {
                ExpandCapacity();
            }

            _afterViewCurrent++;//global tail
            _size++;

            if (_afterViewCurrent > _view.GlobalHeadIndex && _afterViewCurrent == _buckets.Length)
            {
                _afterViewCurrent = 0;
            }

            _buckets[_afterViewCurrent].Add(messageInfo);
        }

        public async ValueTask Push(MessageInfo messageInfo)
        {
            if (_afterViewCurrent != NotHaveAfterView)
            {
                AddAfterView(messageInfo);
                return;
            }

            if (await TryAddInView(messageInfo))
            {
                return;
            }

            ExpandCapacity();
            _afterViewCurrent = _view.GlobalTailIndex + 1;
            AddAfterView(messageInfo);
            _view.ClusterView();
        }

        public int TryPop()
        {
            var globalHead = _view.GlobalHeadIndex;
            var head = _buckets[globalHead];
            if(head.HavePlace)
            {
                return -1;
            }

            var bucketId = head.BucketId;
            head.Clear();

            if (_afterViewCurrent == NotHaveAfterView)
            {
                var nextBucket = _view.AfterCurrentPeek();
                nextBucket.BucketId = bucketId;
                return bucketId;
            }

            throw new NotImplementedException();

            var globalTail = _afterViewCurrent != NotHaveAfterView ? _afterViewCurrent : _view.GlobalTailIndex;

            int newHead;
            int newTail;
            int newCurrent;

            if(globalTail > globalHead)
            {
                _view = new InFlyView(
                    buckets: _buckets,
                    head: _view.GlobalHeadIndex,
                    tail: _view.GlobalTailIndex,
                    current: _view.GlobalCurrentIndex,
                    size: _view.Size
                    );
            }
            else
            {

            }

            throw new InvalidOperationException();
        }
    }
}