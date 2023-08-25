using System;
using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public ref struct UniqueSortedArray
    {
        private long[] _data;
        private Span<long> _dataSpan;
        private int _size;

        public UniqueSortedArray()
        {
            _data = new long[10];
            _dataSpan = _data.AsSpan();
            _size = 0;
        }

        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _size;
        }

        public void Add(long item)
        {
            if (_size >= _dataSpan.Length)
            {
                IncreaseCapacity();
            }

            int i;
            //right shift
            for (i = _size - 1; i >= 0; i--)
            {
                if (_dataSpan[i] == item)
                {
                    throw new Exception("New element already contains in array, array is corrupted");
                }

                if (_dataSpan[i] < item)
                {
                    break;
                }

                _dataSpan[i + 1] = _dataSpan[i];
            }

            _dataSpan[i + 1] = item;
            _size++;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void IncreaseCapacity()
        {
            var oldData = _data;
            var oldSpan = _dataSpan;
            _data = new long[oldData.Length * 2];
            _dataSpan = _data.AsSpan();

            oldSpan.CopyTo(_dataSpan);
        }

        public ref long this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (index >= _size)
                    throw new IndexOutOfRangeException();

                return ref Unsafe.Add(ref _dataSpan.GetPinnableReference(), (nint)index);
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

            public ref long Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => ref _array[_index];
            }
        }
    }
}