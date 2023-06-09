﻿using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class RequestAwaiterAttribute : Attribute
    {
        public RequestAwaiterAttribute(
            bool useLogger = true,
            bool customOutcomeHeader = false,
            bool customHeaders = false
            )
        {
        }
    }
}