using System;

namespace KafkaExchanger.Attributes.Enums
{
    [Flags]
    public enum OrderMatters
    {
        NotMatters = 0,
        ForProcess = 2,
        ForResponse = 4,
    }
}