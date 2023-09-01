using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class Bucket
    {
        private Dictionary<string, MessageInfo> _data;
        private int _maxSize;

        private int _finished = 0;

        public Bucket(int maxItems, int offsetSize)
        {
            _data = new Dictionary<string, MessageInfo>(maxItems);
            _minOffset = new long[offsetSize];
            _maxOffset = new long[offsetSize];
            for (int i = 0; i < offsetSize; i++)
            {
                _minOffset[i] = -1;
                _maxOffset[i] = -1;
            }
            _maxSize = maxItems;
        }

        public bool HavePlace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _data.Count < _maxSize;
        }

        public int BucketId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set;
        } = -1;

        public Dictionary<string, MessageInfo> Messages => _data;

        public long[] MinOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _minOffset;
        }
        private readonly long[] _minOffset;

        public long[] MaxOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _maxOffset;
        }
        private readonly long[] _maxOffset;


        /// <summary>
        /// Add new horizon info
        /// </summary>
        /// <returns>Index of new element in storage</returns>
        /// <exception cref="Exception">If item already contains</exception>
        public void Add(string guid, MessageInfo item)
        {
            if (_data.Count == _maxSize)
            {
                throw new Exception("Item limit exceeded");
            }

            _data[guid] = item;
        }

        public void SetOffset(
            string guid, 
            int offsetId,
            Confluent.Kafka.TopicPartitionOffset offset
            )
        {
            if (!_data.TryGetValue(guid, out var result))
            {
                throw new Exception("Guid not found");
            }

            result.SetOffset(offsetId, offset);
            var offsetVal = offset.Offset.Value;
            var min = _minOffset[offsetId];
            if(min == -1 || offsetVal < min)
            {
                _minOffset[offsetId] = offsetVal;
            }

            var max = _maxOffset[offsetId];
            if(max == -1 || offsetVal > max)
            {
                _maxOffset[offsetId] = offsetVal;
            }
        }

        public MessageInfo Finish(string guid)
        {
            if (!_data.TryGetValue(guid, out var result))
            {
                throw new Exception("Guid not found");
            }

            if (result.Finished)
            {
                return result;
            }

            result.Finish();
            _finished++;

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CanFree()
        {
            return _maxSize == _data.Count && _maxSize == _finished;
        }

        public bool IsEmpty()
        {
            return _data.Count == 0;
        }

        public void Reset()
        {
            _data.Clear();
            _finished = 0;
        }
    }
}