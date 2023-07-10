using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class OutcomeAttribute : Attribute
    {
        public OutcomeAttribute(
            Type incomeKeyType,
            Type incomeValueType,
            bool useLogger = true,
            bool customOutcomeHeader = false,
            bool customHeaders = false
            )
        {
        }
    }
}