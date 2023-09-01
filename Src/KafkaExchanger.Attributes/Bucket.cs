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
        private int _offsetsFull = 0;

        public Bucket(int maxItems, int offsetSize)
        {
            _data = new Dictionary<string, MessageInfo>(maxItems);
            _minOffset = new Confluent.Kafka.TopicPartitionOffset[offsetSize];
            _maxOffset = new Confluent.Kafka.TopicPartitionOffset[offsetSize];
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

        public Confluent.Kafka.TopicPartitionOffset[] MinOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _minOffset;
        }
        private readonly Confluent.Kafka.TopicPartitionOffset[] _minOffset;

        public Confluent.Kafka.TopicPartitionOffset[] MaxOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _maxOffset;
        }
        private readonly Confluent.Kafka.TopicPartitionOffset[] _maxOffset;


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
            if(min == null || offsetVal < min.Offset.Value)
            {
                _minOffset[offsetId] = offset;
            }

            var max = _maxOffset[offsetId];
            if(max == null || offsetVal > max.Offset.Value)
            {
                _maxOffset[offsetId] = offset;
            }

            var allInputDone = false;
            for (int i = 0; i < result.TopicPartitionOffset.Length; i++)
            {
                allInputDone &= result.TopicPartitionOffset[i] != null;
            }
            if(allInputDone)
            {
                _offsetsFull++;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool OnlyWaitFinish()
        {
            return _offsetsFull == _maxSize;
        }

        public bool IsEmpty()
        {
            return _data.Count == 0;
        }

        public void Reset()
        {
            _data.Clear();
            Array.Clear(_minOffset);
            Array.Clear(_maxOffset);
            _finished = 0;
            _offsetsFull = 0;
        }
    }
}