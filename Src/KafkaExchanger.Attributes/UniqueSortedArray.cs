using System;
using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class UniqueSortedArray
    {
        private long[] _data;
        private int _size;

        public UniqueSortedArray()
        {
            _data = new long[10];
            _size = 0;
        }

        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _size;
        }

        public void Add(long item)
        {
            if (_size >= _data.Length)
            {
                IncreaseCapacity();
            }

            int i;
            //right shift
            for (i = _size - 1; i >= 0; i--)
            {
                if (_data[i] == item)
                {
                    throw new Exception("New element already contains in array, array is corrupted");
                }

                if (_data[i] < item)
                {
                    break;
                }

                _data[i + 1] = _data[i];
            }

            _data[i + 1] = item;
            _size++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void IncreaseCapacity()
        {
            var oldData = _data;
            _data = new long[oldData.Length * 2];

            oldData.CopyTo(_data, 0);
        }

        public long this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (index >= _size)
                    throw new IndexOutOfRangeException();

                return _data[index];
            }
        }

        public UniqueSortedArray.Enumerator GetEnumerator() => new UniqueSortedArray.Enumerator(this);

        public ref struct Enumerator
        {
            private readonly UniqueSortedArray _array;
            private int _index;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal Enumerator(UniqueSortedArray array)
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

            public long Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _array[_index];
            }
        }
    }
}