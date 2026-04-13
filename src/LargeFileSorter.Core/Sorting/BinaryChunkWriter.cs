using System.Text;

namespace LargeFileSorter.Core;

/// <summary>
/// Writes sorted LineEntry arrays to binary chunk files.
/// Format per record: Int64 (Number) + length-prefixed UTF-8 string (Text).
/// Binary format avoids text re-parsing during the merge phase.
/// </summary>
internal static class BinaryChunkWriter
{
    public static void Write(string path, LineEntry[] data, int count)
    {
        using var stream = new FileStream(path, FileMode.Create, FileAccess.Write,
            FileShare.None, 65536, FileOptions.SequentialScan);
        using var bw = new BinaryWriter(stream, new UTF8Encoding(false), leaveOpen: false);

        for (var i = 0; i < count; i++)
        {
            bw.Write(data[i].Number);
            bw.Write(data[i].Text);
        }
    }

    /// <summary>
    /// Converts a single binary chunk directly to the text output format.
    /// Used as a fast path when the input fits in a single chunk (no merge needed).
    /// </summary>
    public static void ConvertToText(string binaryChunkPath, string outputPath, int bufferSize)
    {
        using var inStream = new FileStream(binaryChunkPath, FileMode.Open, FileAccess.Read,
            FileShare.Read, bufferSize, FileOptions.SequentialScan);
        using var br = new BinaryReader(inStream, new UTF8Encoding(false), leaveOpen: false);

        using var outStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
            FileShare.None, bufferSize, FileOptions.SequentialScan);
        using var writer = new StreamWriter(outStream, new UTF8Encoding(false), bufferSize);

        try
        {
            while (true)
            {
                var number = br.ReadInt64();
                var text = br.ReadString();
                writer.Write(number);
                writer.Write(". ");
                writer.WriteLine(text);
            }
        }
        catch (EndOfStreamException) { }
    }
}
