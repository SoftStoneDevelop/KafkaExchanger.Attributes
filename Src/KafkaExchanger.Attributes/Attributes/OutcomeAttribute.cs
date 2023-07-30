using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public sealed class OutputAttribute : Attribute
    {
        public OutputAttribute(
            Type keyType,
            Type valueType
            )
        {
        }
    }
}