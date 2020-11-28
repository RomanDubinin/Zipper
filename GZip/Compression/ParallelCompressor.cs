using System;
using System.Buffers;
using System.IO;
using GZip.Infrastructure;

namespace GZip.Compression
{
    public class ParallelCompressor : IPartiallyParallelizableJob
    {
        private readonly int blockSize;

        private readonly Stream inputStream;
        private readonly Stream outputStream;

        private readonly BlockingQueue<DataBlock> inputQueue;
        private readonly BlockingQueue<DataBlock> outputQueue;

        private readonly ObjectPool<DataBlock> dataBlocksPool;
        private readonly ArrayPool<byte> byteArrayPool;

        private readonly Compressor compressor;

        private readonly byte[] compressorHeader;

        private const int retryCount = 4;

        public ParallelCompressor(Stream inputStream,
                                  Stream outputStream,
                                  int blockSize,
                                  byte[] compressorHeader,
                                  BlockingQueue<DataBlock> inputQueue,
                                  BlockingQueue<DataBlock> outputQueue,
                                  ObjectPool<DataBlock> dataBlocksPool,
                                  ArrayPool<byte> byteArrayPool,
                                  Compressor compressor)
        {
            this.inputStream = inputStream;
            this.outputStream = outputStream;
            this.blockSize = blockSize;
            this.compressorHeader = compressorHeader;
            this.inputQueue = inputQueue;
            this.outputQueue = outputQueue;
            this.dataBlocksPool = dataBlocksPool;
            this.byteArrayPool = byteArrayPool;
            this.compressor = compressor;
        }

        public void JobStart()
        {
            var blocksCount = GetBlocksCount();
            for (var i = 0; i < blocksCount; i++)
            {
                var data = byteArrayPool.Rent(blockSize);
                var blockLength = inputStream.Read(data, 0, blockSize);

                var dataBlock = dataBlocksPool.Get();
                dataBlock.Data = data;
                dataBlock.Length = blockLength;
                dataBlock.Number = i;
                inputQueue.Enqueue(dataBlock);
            }
            inputQueue.Finish();
        }

        public void ParallelizableJob()
        {
            while (inputQueue.Dequeue(out var dataBlock))
            {
                if (!TryCompress(dataBlock, out var compressedData, out var compressedDataLen))
                    throw new InvalidDataException($"Cannot compress data block {dataBlock.Number}");
                byteArrayPool.Return(dataBlock.Data);
                dataBlock.Data = compressedData;
                dataBlock.Length = compressedDataLen;
                if (!outputQueue.Enqueue(dataBlock))
                    return;
            }
        }

        public void AfterAllParallelJobs()
        {
            outputQueue.Finish();
        }

        public void JobEnd()
        {
            var blocksCount = GetBlocksCount();

            outputStream.Write(compressorHeader);
            outputStream.Write(BitConverter.GetBytes(inputStream.Length));
            outputStream.Write(BitConverter.GetBytes(blocksCount));

            while (outputQueue.Dequeue(out var dataBlock))
            {
                outputStream.Write(BitConverter.GetBytes(dataBlock.Number));
                outputStream.Write(BitConverter.GetBytes(dataBlock.Length));
                outputStream.Write(dataBlock.Data, 0, dataBlock.Length);
                outputStream.Write(BitConverter.GetBytes(HashCalculator.GetDataBlockHashCode(dataBlock.Data, dataBlock.Length, dataBlock.Number)));
                byteArrayPool.Return(dataBlock.Data);
                dataBlocksPool.Return(dataBlock);
            }

            outputStream.Flush();
        }

        public void EmergencyStop()
        {
            inputQueue.Finish();
            outputQueue.Finish();
        }

        private bool TryCompress(DataBlock dataBlock, out byte[] compressedData, out int compressedDataLen)
        {
            var arrayLength = dataBlock.Length;
            for (var i = 0; i < retryCount; i++)
            {
                try
                {
                    compressedData = byteArrayPool.Rent(arrayLength);
                    compressedDataLen = compressor.Compress(dataBlock.Data, dataBlock.Length, compressedData);
                    return true;
                }
                catch (ArgumentException)
                {
                    arrayLength *= 2;
                }
            }

            compressedData = null;
            compressedDataLen = 0;
            return false;
        }

        private long GetBlocksCount()
        {
            return inputStream.Length / blockSize +
                   (inputStream.Length % blockSize == 0 ? 0 : 1);
        }
    }
}