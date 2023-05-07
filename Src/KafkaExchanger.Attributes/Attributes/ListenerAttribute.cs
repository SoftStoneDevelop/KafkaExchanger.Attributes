using System;

namespace KafkaExchanger.Attributes.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class ListenerAttribute : Attribute
    {
        public ListenerAttribute(
            Type incomeKeyType,
            Type incomeValueType,
            bool useLogger = true
            )
        {
        }
    }
}
