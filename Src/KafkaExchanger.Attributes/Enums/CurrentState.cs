﻿namespace KafkaExchanger.Attributes.Enums
{
    public enum CurrentState
    {
        NewMessage = 0,
        AnswerСreated = 1,
        AnswerSended = 2,
    }

    public enum RAState
    {
        Default = 0,
        NotSended = 1,
        Sended = 2,
        AnswerProcessed = 3,
    }
}