using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class MessageInfo
    {
        public MessageInfo(int offsetsSize)
        {
            _offsets = new Confluent.Kafka.TopicPartitionOffset[offsetsSize];
        }

        public bool Finished
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _finished;
        }
        private bool _finished;

        public Confluent.Kafka.TopicPartitionOffset[] TopicPartitionOffset
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _offsets;
        }
        private Confluent.Kafka.TopicPartitionOffset[] _offsets;

        private object _process;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetProcess(object process)
        {
            _process = process;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object TakeProcess()
        {
            var result = _process;
            _process = null;
            return result;
        }

        public bool HaveProcess
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _process != null;
        }

        public void SetOffset(int id, Confluent.Kafka.TopicPartitionOffset offset)
        {
            _offsets[id] = offset;
        }

        public void Finish()
        {
            _finished = true;
        }
    }
}