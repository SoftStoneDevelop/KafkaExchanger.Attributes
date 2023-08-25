using System;
using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class HorizonStorage
    {
        private long[] _data;
        private int _size;

        public HorizonStorage()
        {
            _data = new long[10];
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
        public int Add(long item)
        {
            if (_size >= _data.Length)
            {
                IncreaseCapacity();
            }

            int i = _size - 1;
            //right shift
            for (; i >= 0; i--)
            {
                if (_data[i] == item)
                {
                    throw new Exception("New element already contains, HorizonStorage is corrupted");
                }

                if (_data[i] > item)
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
                int order = _data[i].CompareTo(horizonId);

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
            var horizonId = _data[index];
            _size = index;

            return horizonId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void IncreaseCapacity()
        {
            var oldData = _data;
            _data = new long[oldData.Length * 2];

            oldData.CopyTo(_data, 0);
        }

        public ref long this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (index >= _size)
                    throw new IndexOutOfRangeException();

                return ref _data[index];
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

            public ref long Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => ref _array[_index];
            }
        }
    }
}