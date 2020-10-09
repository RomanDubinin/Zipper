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
            var originalData = new byte[] {1, 2, 3, 4, 5, 6};
            var inputStreamForCompression = new MemoryStream(originalData);
            var outputStreamForCompression = new MemoryStream();
            var compressor = new ParallelCompressor(3, 1, inputStreamForCompression, outputStreamForCompression, header);
            compressor.CompressParallel();
            var compressedData = outputStreamForCompression.ToArray();

            var inputStreamForDecompression = new MemoryStream(compressedData);
            var outputStreamForDecompression = new MemoryStream();
            var decompressor = new ParallelCompressor(4, 1, inputStreamForDecompression, outputStreamForDecompression, header);
            decompressor.DecompressParallel();
            var decompressedData = outputStreamForDecompression.ToArray();

            Assert.AreEqual(originalData, decompressedData);
        }
    }
}