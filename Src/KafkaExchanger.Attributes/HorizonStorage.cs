using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class HorizonInfo
    {
        public HorizonInfo(
            long horizonId
            )
        {
            _horizonId = horizonId;
        }

        public long HorizonId 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _horizonId;
        }
        private long _horizonId;

        public bool Finished
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _finished; 
        }
        private bool _finished;

        public ReadOnlyCollection<Confluent.Kafka.TopicPartitionOffset> TopicPartitionOffset 
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _topicPartitionOffset.AsReadOnly(); 
        }
        private List<Confluent.Kafka.TopicPartitionOffset> _topicPartitionOffset;

        public void AddOffset(Confluent.Kafka.TopicPartitionOffset offset)
        {
            if(_finished)
            {
                throw new InvalidOperationException("Can not add offset into finished horizon");
            }

            _topicPartitionOffset ??= new();
            _topicPartitionOffset.Add(offset);
        }

        public void Finish()
        {
            _finished = true;
        }
    }

    public class HorizonStorage
    {
        private HorizonInfo[] _data;
        private int _size;

        /// <summary>
        /// -1 means all storage not contain unfinished
        /// </summary>
        private int _minHorizonIndex = -1;

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

            if (_size != 0 && i + 1 == _size)
            {
                throw new Exception("The new horizon must be higher than the existing one");
            }

            _data[i + 1] = item;
            _minHorizonIndex++;
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

        public void Finish(long horizonId)
        {
            var index = Find(horizonId);
            _data[index].Finish();

            if(index != _minHorizonIndex)
            {
                return;
            }

            int i;
            for (i = _minHorizonIndex - 1; i >= 0; i--)
            {
                if (!_data[i].Finished)
                {
                    break;
                }
            }

            _minHorizonIndex = i;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CanFree()
        {
            return _size - (_minHorizonIndex + 1);
        }

        public void ClearFinished()
        {
            var canFree = CanFree();
            if (canFree == 0)
            {
                throw new Exception("Nothing to clear");
            }

            Array.Clear(array: _data, index: _minHorizonIndex + 1, length: canFree);
            _size -= canFree;
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