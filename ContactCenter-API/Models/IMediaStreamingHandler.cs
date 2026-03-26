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

        /// <summary>
        /// Feed a complete TTS audio blob (raw PCM16 at 16 kHz mono) directly into
        /// the jitter buffer.  No resampling is applied — the bytes must already be
        /// at the codec rate expected by FreeSWITCH (16 kHz / 16-bit / mono).
        /// 
        /// The jitter buffer must have been started by NotifyAiResponseStarted()
        /// before calling this method.  Call FlushAudioAsync() afterwards to drain.
        /// Default no-op for handlers that do not support direct PCM injection.
        /// </summary>
        Task FeedPcm16AudioAsync(byte[] pcm16_16kHz) => Task.CompletedTask;
    }
}
