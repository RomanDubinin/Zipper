using System;
using System.Buffers;
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
            var blockSize = 1024;

            var inputStreamForCompression = new MemoryStream(originalData);
            var outputStreamForCompression = new MemoryStream();
            var parallelCompressor = MakeParallelCompressor(3, blockSize, inputStreamForCompression, outputStreamForCompression, header);
            parallelCompressor.CompressParallel();
            var compressedData = outputStreamForCompression.ToArray();

            var inputStreamForDecompression = new MemoryStream(compressedData);
            var outputStreamForDecompression = new MemoryStream();
            var decompressor = MakeParallelCompressor(4, blockSize, inputStreamForDecompression, outputStreamForDecompression, header);
            decompressor.DecompressParallel();
            var decompressedData = outputStreamForDecompression.ToArray();

            Assert.AreEqual(originalData, decompressedData);
        }

        private ParallelCompressor MakeParallelCompressor(int coresNumber, int blockSize, MemoryStream inputStreamForCompression, MemoryStream outputStreamForCompression, byte[] header)
        {
            var inputQueue = new BlockingQueue<DataBlock>(coresNumber);
            var outputQueue = new BlockingQueue<DataBlock>(coresNumber);
            var dataBlocksPool = new ObjectPool<DataBlock>(() => new DataBlock());
            var byteArrayPool = ArrayPool<byte>.Create(blockSize * 2, coresNumber * 2);
            var compressor = new Compressor();
            var threadRunner = new ThreadRunner();
            return new ParallelCompressor(coresNumber, blockSize, inputStreamForCompression, outputStreamForCompression, header, inputQueue, outputQueue, dataBlocksPool, byteArrayPool, compressor, threadRunner);
        }
    }
}