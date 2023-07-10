using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class ListenerAttribute : Attribute
    {
        public ListenerAttribute(
            bool useLogger = true
            )
        {
        }
    }
}