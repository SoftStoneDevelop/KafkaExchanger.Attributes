using System;
using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class HorizonInfo
    {
        public HorizonInfo(
            long horizonId, 
            Confluent.Kafka.TopicPartitionOffset topicPartitionOffset
            )
        {
            HorizonId = horizonId;
            TopicPartitionOffset = topicPartitionOffset;
        }

        public long HorizonId { get; init; }

        public Confluent.Kafka.TopicPartitionOffset TopicPartitionOffset { get; init; }
    }

    public class HorizonStorage
    {
        private HorizonInfo[] _data;
        private int _size;

        public HorizonStorage()
        {
            _data = new HorizonInfo[10];
            _size = 0;
        }

        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _size;
        }

        /// <summary>
        /// Add new horizon info
        /// </summary>
        /// <returns>Index of new element in storage</returns>
        /// <exception cref="Exception">If iitem already contains</exception>
        public int Add(HorizonInfo item)
        {
            if (_size >= _data.Length)
            {
                IncreaseCapacity();
            }

            int i = _size - 1;
            //right shift
            for (; i >= 0; i--)
            {
                if (_data[i].HorizonId == item.HorizonId)
                {
                    throw new Exception("New element already contains, HorizonStorage is corrupted");
                }

                if (_data[i].HorizonId > item.HorizonId)
                {
                    break;
                }

                _data[i + 1] = _data[i];
            }

            _data[i + 1] = item;
            _size++;

            return i + 1;
        }

        public int Find(long horizonId)
        {
            int lo = 0;
            int hi = _size - 1;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);
                int order = _data[i].HorizonId.CompareTo(horizonId);

                if (order == 0)
                {
                    return i;
                }

                if (order < 0)
                {
                    hi = i - 1;
                }
                else
                {
                    lo = i + 1;
                }
            }

            throw new Exception("HorizonId not found");
        }

        public int CanFree(int index)
        {
            return _size - index;
        }

        public long Clear(int index)
        {
            if(_size == 0)
            {
                throw new Exception("Storage is empty");
            }

            var horizonId = _data[index].HorizonId;
            _size = index;

            var freeLength = _data.Length - _size;
            if (freeLength > 0)
            {
                Array.Clear(array: _data, index: _size, length: freeLength);
            }

            return horizonId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void IncreaseCapacity()
        {
            var oldData = _data;
            _data = new HorizonInfo[oldData.Length * 2];
            oldData.CopyTo(_data, 0);
        }

        public HorizonInfo this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (index >= _size)
                    throw new IndexOutOfRangeException();

                return _data[index];
            }
        }

        public HorizonStorage.Enumerator GetEnumerator() => new HorizonStorage.Enumerator(this);

        public ref struct Enumerator
        {
            private readonly HorizonStorage _array;
            private int _index;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal Enumerator(HorizonStorage array)
            {
                _array = array;
                _index = _array.Size;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                int index = _index - 1;
                if (index > 0)
                {
                    _index = index;
                    return true;
                }

                return false;
            }

            public HorizonInfo Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _array[_index];
            }
        }
    }
}