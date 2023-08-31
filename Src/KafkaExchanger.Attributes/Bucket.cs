using System;
using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class Bucket
    {
        private MessageInfo[] _data;
        private int _size;

        private int _finished = 0;

        public Bucket(int maxItems)
        {
            _data = new MessageInfo[maxItems];
            _size = 0;
        }

        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _size;
        }

        public bool HavePlace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _size < _data.Length;
        }

        public int BucketId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set;
        } = -1;

        public MessageInfo[] Messages => _data;

        /// <summary>
        /// Add new horizon info
        /// </summary>
        /// <returns>Index of new element in storage</returns>
        /// <exception cref="Exception">If item already contains</exception>
        public void Add(MessageInfo item)
        {
            if (_size >= _data.Length)
            {
                throw new Exception("Item limit exceeded");
            }

            item.Id = _size++;
            _data[item.Id] = item;
        }

        public int Find(int messageId)
        {
            int lo = 0;
            int hi = _size - 1;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);
                int order = _data[i].Id.CompareTo(messageId);

                if (order == 0)
                {
                    return i;
                }

                if (order < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }

            throw new Exception("Id not found");
        }

        public MessageInfo Finish(int messageId, Confluent.Kafka.TopicPartitionOffset[] offsets)
        {
            var index = Find(messageId);
            var result = _data[index];

            if (result.Finished)
            {
                return result;
            }

            result.Finish(offsets);
            _finished++;

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CanFree()
        {
            return _size == _data.Length && _data.Length == _finished;
        }

        public MessageInfo[] ResetMessages()
        {
            var result = _data;
            _data = new MessageInfo[_data.Length];
            _size = 0;
            _finished = 0;
            return result;
        }

        public MessageInfo[] SetMessages(Bucket bucket)
        {
            var result = _data;
            _data = bucket._data;
            _size = bucket._size;
            _finished = bucket._finished;

            bucket.Clear();
            return result;
        }

        private void Clear()
        {
            _data = null;
            _size = 0;
            _finished = 0;
            BucketId = -1;
        }

        public MessageInfo this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (index >= _size)
                    throw new IndexOutOfRangeException();

                return _data[index];
            }
        }

        public Bucket.Enumerator GetEnumerator() => new Bucket.Enumerator(this);

        public ref struct Enumerator
        {
            private readonly Bucket _array;
            private int _index;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal Enumerator(Bucket array)
            {
                _array = array;
                _index = -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                int index = _index + 1;
                if (index < _array.Size)
                {
                    _index = index;
                    return true;
                }

                return false;
            }

            public MessageInfo Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _array[_index];
            }
        }
    }
}