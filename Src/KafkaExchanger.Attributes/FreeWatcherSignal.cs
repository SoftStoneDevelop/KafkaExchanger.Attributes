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
            if (result != 1)
            {
                return;
            }

            while (true)
            {
                var tcs = m_tcs;
                if (tcs.TrySetResult(true))
                {
                    break;
                }
            }
        }
        private volatile TaskCompletionSource<bool> m_tcs;

        public void SignalStuck()
        {
            var result = Interlocked.Decrement(ref _haveFree);
            if(result != 0)
            {
                return;
            }

            while (true)
            {
                var tcs = m_tcs;
                if (!tcs.Task.IsCompleted || Interlocked.CompareExchange(ref m_tcs, new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously), tcs) == tcs)
                {
                    break;
                }
            }
        }

        public Task WaitFree()
        {
            return m_tcs.Task;
        }
    }
}