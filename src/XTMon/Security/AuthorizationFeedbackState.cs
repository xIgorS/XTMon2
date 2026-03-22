namespace XTMon.Security;

public sealed class AuthorizationFeedbackState
{
    public bool HasInfrastructureFailure { get; private set; }

    public string? UserMessage { get; private set; }

    public void Clear()
    {
        HasInfrastructureFailure = false;
        UserMessage = null;
    }

    public void SetInfrastructureFailure(string userMessage)
    {
        HasInfrastructureFailure = true;
        UserMessage = userMessage;
    }
}