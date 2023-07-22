using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public sealed class IncomeAttribute : Attribute
    {
        public IncomeAttribute(
            Type keyType,
            Type valueType
            )
        {
        }
    }
}