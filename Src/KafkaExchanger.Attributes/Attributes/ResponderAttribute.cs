using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class ResponderAttribute : Attribute
    {
        public ResponderAttribute(
            Type outcomeKeyType,
            Type outcomeValueType,
            Type incomeKeyType,
            Type incomeValueType
            )
        {
        }
    }
}
