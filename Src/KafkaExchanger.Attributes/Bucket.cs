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

        public Bucket(int maxItems)
        {
            _data = new Dictionary<string, MessageInfo>(maxItems);
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

        public MessageInfo Finish(string guid, Confluent.Kafka.TopicPartitionOffset[] offsets)
        {
            if (!_data.TryGetValue(guid, out var result))
            {
                throw new Exception("Guid not found");
            }

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
            return _maxSize == _data.Count && _maxSize == _finished;
        }

        public Dictionary<string, MessageInfo> ResetMessages()
        {
            var result = _data;
            _data = new Dictionary<string, MessageInfo>(_data.Count);
            _finished = 0;
            return result;
        }

        public Dictionary<string, MessageInfo> SetMessages(Bucket bucket)
        {
            var result = _data;
            _data = bucket._data;
            _finished = bucket._finished;

            bucket.Clear();
            return result;
        }

        private void Clear()
        {
            _data = null;
            _finished = 0;
            BucketId = -1;
        }
    }
}