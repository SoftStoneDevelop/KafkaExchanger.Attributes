using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class ResponderAttribute : Attribute
    {
        public ResponderAttribute(
            bool useLogger = true,
            uint commitAfter = 1,
            bool checkCurrentState = false,
            bool useAfterSendResponse = false,
            bool useAfterCommit = false
            )
        {
        }
    }
}