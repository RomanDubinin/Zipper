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
        public void Test(byte[] originalData)
        {
            var compressed = compressor.Compress(originalData, originalData.Length);
            var decompressed = compressor.Decompress(compressed, compressed.Length);
            Assert.AreEqual(originalData, decompressed);
        }
    }
}