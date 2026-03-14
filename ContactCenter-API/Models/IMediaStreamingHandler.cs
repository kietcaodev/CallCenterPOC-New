namespace ContactCenterPOC.Models
{
    /// <summary>
    /// Interface for media streaming handlers. Both AcsMediaStreamingHandler 
    /// and FreeSwitchMediaHandler implement this to work with AI services.
    /// </summary>
    public interface IMediaStreamingHandler
    {
        Task SendMessageAsync(string message);
    }
}
