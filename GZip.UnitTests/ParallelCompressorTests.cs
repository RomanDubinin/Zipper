using System;
using System.IO;
using NUnit.Framework;

namespace GZip.UnitTests
{
    [TestFixture]
    public class ParallelCompressorTests
    {
        [Test]
        public void TestCompressAndDecompress()
        {
            var header = new byte[] {7, 7, 7};
            var random = new Random();
            var originalData = new byte[5 * 1024 * 1024];
            random.NextBytes(originalData);
            var inputStreamForCompression = new MemoryStream(originalData);
            var outputStreamForCompression = new MemoryStream();
            var compressor = new ParallelCompressor(3, 1024, inputStreamForCompression, outputStreamForCompression, header);
            compressor.CompressParallel();
            var compressedData = outputStreamForCompression.ToArray();

            var inputStreamForDecompression = new MemoryStream(compressedData);
            var outputStreamForDecompression = new MemoryStream();
            var decompressor = new ParallelCompressor(4, 1024, inputStreamForDecompression, outputStreamForDecompression, header);
            decompressor.DecompressParallel();
            var decompressedData = outputStreamForDecompression.ToArray();

            Assert.AreEqual(originalData, decompressedData);
        }
    }
}