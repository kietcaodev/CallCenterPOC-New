namespace ContactCenterPOC.Models
{
    /// <summary>
    /// Interface for media streaming handlers. Both AcsMediaStreamingHandler 
    /// and FreeSwitchMediaHandler implement this to work with AI services.
    /// </summary>
    public interface IMediaStreamingHandler
    {
        Task SendMessageAsync(string message);
        Task FlushAudioAsync() => Task.CompletedTask;

        /// <summary>
        /// Notify that the AI model has started generating a response.
        /// Handler should mute upstream mic until NotifyAiResponseFinished.
        /// </summary>
        void NotifyAiResponseStarted() { }

        /// <summary>
        /// Notify that the AI model has finished generating the current response.
        /// Handler can unmute mic after playback completes.
        /// </summary>
        void NotifyAiResponseFinished() { }
    }
}
