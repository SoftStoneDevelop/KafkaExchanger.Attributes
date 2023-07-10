using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class OutcomeAttribute : Attribute
    {
        public OutcomeAttribute(
            Type keyType,
            Type valueType
            )
        {
        }
    }
}