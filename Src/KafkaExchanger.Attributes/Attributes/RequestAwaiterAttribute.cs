using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class RequestAwaiterAttribute : Attribute
    {
        public RequestAwaiterAttribute(
            Type outcomeKeyType,
            Type outcomeValueType,
            Type incomeKeyType,
            Type incomeValueType
            )
        {
        }
    }
}