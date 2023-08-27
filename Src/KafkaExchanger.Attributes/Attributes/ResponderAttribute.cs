using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class ResponderAttribute : Attribute
    {
        public ResponderAttribute(
            bool useLogger = false,
            bool checkCurrentState = false,
            bool afterSend = false,
            bool afterCommit = false
            )
        {
        }
    }
}