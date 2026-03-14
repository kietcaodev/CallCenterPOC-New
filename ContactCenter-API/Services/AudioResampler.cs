namespace ContactCenterPOC.Services
{
    /// <summary>
    /// Resamples PCM16 audio between FreeSWITCH (16kHz) and OpenAI Realtime (24kHz).
    /// Uses simple linear interpolation for upsampling and averaging for downsampling.
    /// </summary>
    public static class AudioResampler
    {
        /// <summary>
        /// Resample PCM16 audio from sourceRate to targetRate.
        /// Input/output: 16-bit signed little-endian PCM mono.
        /// </summary>
        public static byte[] Resample(byte[] input, int sourceRate, int targetRate)
        {
            if (sourceRate == targetRate) return input;

            int sampleCount = input.Length / 2; // 16-bit = 2 bytes per sample
            double ratio = (double)sourceRate / targetRate;
            int outputSampleCount = (int)(sampleCount / ratio);

            var output = new byte[outputSampleCount * 2];

            for (int i = 0; i < outputSampleCount; i++)
            {
                double srcIndex = i * ratio;
                int index0 = (int)srcIndex;
                int index1 = Math.Min(index0 + 1, sampleCount - 1);
                double frac = srcIndex - index0;

                short sample0 = BitConverter.ToInt16(input, index0 * 2);
                short sample1 = BitConverter.ToInt16(input, index1 * 2);

                short interpolated = (short)(sample0 + (sample1 - sample0) * frac);

                BitConverter.TryWriteBytes(output.AsSpan(i * 2), interpolated);
            }

            return output;
        }

        /// <summary>
        /// Upsample from 16kHz to 24kHz (FreeSWITCH → OpenAI).
        /// </summary>
        public static byte[] Upsample16kTo24k(byte[] input) => Resample(input, 16000, 24000);

        /// <summary>
        /// Downsample from 24kHz to 16kHz (OpenAI → FreeSWITCH).
        /// </summary>
        public static byte[] Downsample24kTo16k(byte[] input) => Resample(input, 24000, 16000);
    }
}
