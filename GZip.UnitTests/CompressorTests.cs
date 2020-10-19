using GZip.Compression;
using NUnit.Framework;

namespace GZip.UnitTests
{
    public class CompressorTests
    {
        private Compressor compressor;

        [SetUp]
        public void Setup()
        {
            compressor = new Compressor();
        }

        [Test]
        [TestCase(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9})]
        [TestCase(new byte[] { })]
        public void TestCompressAndDecompress(byte[] originalData)
        {
            var compressed = new byte[100];
            var compressedLen = compressor.Compress(originalData, originalData.Length, compressed);
            
            var decompressed = new byte[originalData.Length];
            compressor.Decompress(compressed, compressedLen, decompressed);

            Assert.AreEqual(originalData, decompressed);
        }
    }
}