using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace KafkaExchanger
{
    public partial interface IProducerPool<Key, Value>
    {

        public Task Produce(string topicName, Confluent.Kafka.Message<Key, Value> message);

        public Task Produce(Confluent.Kafka.TopicPartition topicPartition, Confluent.Kafka.Message<Key, Value> message);

    }

    public partial class ProducerPool<Key, Value> : IProducerPool<Key, Value>, System.IAsyncDisposable
    {

        private abstract class ProduceInfo
        {
            public Confluent.Kafka.Message<Key, Value> Message;
            public TaskCompletionSource CompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private class ByName : ProduceInfo
        {
            public string Name;
        }

        private class ByTopicPartition : ProduceInfo
        {
            public Confluent.Kafka.TopicPartition TopicPartition;
        }

        public ProducerPool(
            HashSet<string> transactionalIds,
            string bootstrapServers,
            int messagesInTransaction = 100,
            Action<Confluent.Kafka.ProducerConfig> changeConfig = null
            )
        {
            _messagesInTransaction = messagesInTransaction;
            _routines = new Task[transactionalIds.Count];
            var i = 0;
            foreach (var transactionalId in transactionalIds)
            {
                var config = new Confluent.Kafka.ProducerConfig();
                config.SocketTimeoutMs = 5000;
                config.TransactionTimeoutMs = 5000;

                if (changeConfig != null)
                {
                    changeConfig(config);
                }

                config.BootstrapServers = bootstrapServers;
                config.TransactionalId = transactionalId;
                _routines[i++] = ProduceRoutine(config, _cancellationTokenSource.Token);
            }
        }

        private int _messagesInTransaction;
        private Task[] _routines;
        private CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private Channel<ProduceInfo> _produceChannel = Channel.CreateUnbounded<ProduceInfo>(
            new UnboundedChannelOptions
            {
                AllowSynchronousContinuations = false,
                SingleReader = false,
                SingleWriter = false
            });

        private async Task ProduceRoutine(Confluent.Kafka.ProducerConfig config, CancellationToken cancellationToken)
        {
            var reader = _produceChannel.Reader;
            var sendTemp = new List<ProduceInfo>(_messagesInTransaction);
            var producer =
                new Confluent.Kafka.ProducerBuilder<Key, Value>(config)
                .Build()
                ;

            try
            {
                producer.InitTransactions(TimeSpan.FromSeconds(60));
                while (!cancellationToken.IsCancellationRequested)
                {
                    while (sendTemp.Count > 0)
                    {
                        producer.BeginTransaction();
                        for (int i = 0; i < sendTemp.Count; i++)
                        {
                            var sendInfo = sendTemp[i];
                            if (sendInfo is ByName byName)
                            {
                                producer.Produce(byName.Name, byName.Message);
                            }
                            else if (sendInfo is ByTopicPartition byTopicPartition)
                            {
                                producer.Produce(byTopicPartition.TopicPartition, byTopicPartition.Message);
                            }
                        }

                        try
                        {
                            while (true)
                            {
                                try
                                {
                                    producer.CommitTransaction();
                                }
                                catch (KafkaRetriableException)
                                {
                                    continue;
                                }

                                break;
                            }
                        }
                        catch (KafkaTxnRequiresAbortException)
                        {
                            producer.AbortTransaction();
                            continue;
                        }

                        for (int i = 0; i < sendTemp.Count; i++)
                        {
                            var sended = sendTemp[i];
                            sended.CompletionSource.SetResult();
                        }
                        sendTemp.Clear();
                    }

                    var info = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    var sw = Stopwatch.StartNew();
                    sendTemp.Add(info);
                    while (!cancellationToken.IsCancellationRequested && sw.ElapsedMilliseconds < 1 && sendTemp.Count < _messagesInTransaction && reader.TryRead(out info))
                    {
                        sendTemp.Add(info);
                    }
                }
            }
            catch
            {
                //ignore
            }
            finally
            {
                producer.Dispose();
            }

            for (int i = 0; i < sendTemp.Count; i++)
            {
                var sended = sendTemp[i];
                sended.CompletionSource.SetCanceled(cancellationToken);
            }
        }

        public async Task Produce(string topicName, Confluent.Kafka.Message<Key, Value> message)
        {
            var info = new ByName
            {
                Message = message,
                Name = topicName
            };

            await _produceChannel.Writer.WriteAsync(info).ConfigureAwait(false);
            await info.CompletionSource.Task.ConfigureAwait(false);
        }

        public async Task Produce(Confluent.Kafka.TopicPartition topicPartition, Confluent.Kafka.Message<Key, Value> message)
        {
            var info = new ByTopicPartition
            {
                Message = message,
                TopicPartition = topicPartition
            };

            await _produceChannel.Writer.WriteAsync(info).ConfigureAwait(false);
            await info.CompletionSource.Task.ConfigureAwait(false);
        }

        public async ValueTask DisposeAsync()
        {
            _cancellationTokenSource.Cancel();

            _produceChannel.Writer.Complete();
            for (int i = 0; i < _routines.Length; i++)
            {
                try
                {
                    await _routines[i];
                }
                catch
                {
                    //ignore
                }
            }
            _cancellationTokenSource.Dispose();
        }

    }
}