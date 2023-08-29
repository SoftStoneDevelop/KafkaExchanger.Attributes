using System.Runtime.CompilerServices;

namespace KafkaExchanger
{
    public class MessageInfo
    {
        public long Id
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set;
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

        public void Finish(Confluent.Kafka.TopicPartitionOffset[] offsets)
        {
            _finished = true;
            _offsets = offsets;
        }
    }
}