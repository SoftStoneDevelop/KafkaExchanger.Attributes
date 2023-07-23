using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class RequestAwaiterAttribute : Attribute
    {
        public RequestAwaiterAttribute(
            bool useLogger = true,
            uint commitAfter = 1,
            bool checkCurrentState = false,
            bool useAfterCommit = false,
            bool customOutcomeHeader = false,
            bool customHeaders = false
            )
        {
        }
    }
}