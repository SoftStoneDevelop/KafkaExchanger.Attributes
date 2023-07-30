﻿using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class RequestAwaiterAttribute : Attribute
    {
        public RequestAwaiterAttribute(
            bool useLogger = true,
            bool checkCurrentState = false,
            bool useAfterCommit = false,
            bool customOutputHeader = false,
            bool customHeaders = false,
            string[] waitFromService = null
            )
        {
        }
    }
}