using System;

namespace KafkaExchanger.Attributes
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public sealed class InputAttribute : Attribute
    {
        public InputAttribute(
            Type keyType,
            Type valueType,
            string[] waitFromService = null
            )
        {
        }
    }
}