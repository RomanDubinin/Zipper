using System.IO;
using System.IO.Compression;

namespace GZip
{
    public class Compressor
    {
        public byte[] Compress(byte[] data, int length)
        {
            using var compressedStream = new MemoryStream();
            using (var compressionStream = new GZipStream(compressedStream, CompressionMode.Compress))
                compressionStream.Write(data, 0, length);

            return compressedStream.ToArray();
        }

        //todo if decompress bytes that have not been compressed - InvalidDataException
        public byte[] Decompress(byte[] data, int length)
        {
            using var originalStream = new MemoryStream(data, 0, length);
            using var gzipStream = new GZipStream(originalStream, CompressionMode.Decompress);
            using var decompressedStream = new MemoryStream();
            gzipStream.CopyTo(decompressedStream);

            return decompressedStream.ToArray();
        }
    }
}