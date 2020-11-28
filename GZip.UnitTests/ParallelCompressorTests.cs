using System;
using System.Buffers;
using System.IO;
using GZip.Compression;
using GZip.Infrastructure;
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

            var threadRunner = new ThreadRunner();
            var jobRunner = new JobRunner(3, threadRunner);
            var inputStreamForCompression = new MemoryStream(originalData);
            var outputStreamForCompression = new MemoryStream();
            var parallelCompressor = MakeParallelCompressor(3, blockSize, header, inputStreamForCompression, outputStreamForCompression);

            jobRunner.RunParallel(parallelCompressor);

            var compressedData = outputStreamForCompression.ToArray();

            var inputStreamForDecompression = new MemoryStream(compressedData);
            var outputStreamForDecompression = new MemoryStream();
            var decompressor = MakeParallelDecompressor(4, blockSize, header, inputStreamForDecompression, outputStreamForDecompression);
            jobRunner.RunParallel(decompressor);
            var decompressedData = outputStreamForDecompression.ToArray();

            Assert.AreEqual(originalData, decompressedData);
        }

        private ParallelCompressor MakeParallelCompressor(int coresNumber, int blockSize, byte[] header, Stream inputStream, Stream outputStream)
        {
            var inputQueue = new BlockingQueue<DataBlock>(coresNumber);
            var outputQueue = new BlockingQueue<DataBlock>(coresNumber);
            var dataBlocksPool = new ObjectPool<DataBlock>(() => new DataBlock());
            var byteArrayPool = ArrayPool<byte>.Create(blockSize * 2, coresNumber * 2);

            return new ParallelCompressor(inputStream, outputStream, blockSize, header, inputQueue, outputQueue, dataBlocksPool, byteArrayPool);
        }

        private ParallelDecompressor MakeParallelDecompressor(int coresNumber, int blockSize, byte[] header, Stream inputStream, Stream outputStream)
        {
            var inputQueue = new BlockingQueue<DataBlock>(coresNumber);
            var outputQueue = new BlockingQueue<DataBlock>(coresNumber);
            var dataBlocksPool = new ObjectPool<DataBlock>(() => new DataBlock());
            var byteArrayPool = ArrayPool<byte>.Create(blockSize * 2, coresNumber * 2);

            return new ParallelDecompressor(inputStream, outputStream, blockSize, header, inputQueue, outputQueue, dataBlocksPool, byteArrayPool);
        }
    }
}