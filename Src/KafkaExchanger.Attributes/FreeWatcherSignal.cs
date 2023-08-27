using System.Threading;
using System.Threading.Tasks;

namespace KafkaExchanger
{
    public class FreeWatcherSignal
    {
        private int _haveFree;

        public FreeWatcherSignal(int buckets)
        {
            _haveFree = buckets;
            m_tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            m_tcs.TrySetResult(true);
        }

        public void SignalFree()
        {
            var result = Interlocked.Increment(ref _haveFree);
            if (result == 1)
            {
                var tcs = m_tcs;
                tcs.TrySetResult(true);
                Interlocked.CompareExchange(ref m_tcs, new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously), tcs);
            }
        }
        private volatile TaskCompletionSource<bool> m_tcs;

        public void SignalStuck()
        {
            Interlocked.Decrement(ref _haveFree);
        }

        public Task WaitFree()
        {
            return m_tcs.Task;
        }
    }
}