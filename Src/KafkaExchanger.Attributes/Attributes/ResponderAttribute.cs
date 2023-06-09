﻿using KafkaExchanger.Attributes.Enums;
using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class ResponderAttribute : Attribute
    {
        public ResponderAttribute(
            bool useLogger = true,
            uint commitAfter = 1,
            OrderMatters orderMatters = OrderMatters.NotMatters,
            bool checkCurrentState = false,
            bool useAfterSendResponse = false,
            bool useAfterCommit = false,
            bool customOutcomeHeader = false,
            bool customHeaders = false
            )
        {
        }
    }
}