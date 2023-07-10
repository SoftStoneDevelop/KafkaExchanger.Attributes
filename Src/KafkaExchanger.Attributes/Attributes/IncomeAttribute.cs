using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
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