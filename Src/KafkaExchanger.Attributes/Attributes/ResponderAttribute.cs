using KafkaExchanger.Attributes.Enums;
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
            Type incomeValueType,
            bool useLogger = true,
            int commitAfter = 1,
            OrderMatters orderMatters = OrderMatters.NotMatters,
            bool useCheckDuplicate = false,
            bool useBeforeSendResponse = false,
            bool useAfterSendResponse = false,
            bool useAfterCommit = false
            )
        {
        }
    }
}