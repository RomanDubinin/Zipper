using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using GZip.Infrastructure;

namespace GZip.Compression
{
    public class ParallelDecompressor : IPartiallyParallelizableJob
    {
        private readonly Stream inputStream;
        private readonly Stream outputStream;

        private readonly BlockingQueue<DataBlock> inputQueue;
        private readonly BlockingQueue<DataBlock> outputQueue;

        private readonly ObjectPool<DataBlock> dataBlocksPool;
        private readonly ArrayPool<byte> byteArrayPool;

        private readonly Compressor compressor;

        private readonly byte[] compressorHeader;

        public ParallelDecompressor(Stream inputStream,
            Stream outputStream,
            byte[] compressorHeader,
            BlockingQueue<DataBlock> inputQueue,
            BlockingQueue<DataBlock> outputQueue,
            ObjectPool<DataBlock> dataBlocksPool,
            ArrayPool<byte> byteArrayPool,
            Compressor compressor)
        {
            this.inputStream = inputStream;
            this.outputStream = outputStream;
            this.compressorHeader = compressorHeader;
            this.inputQueue = inputQueue;
            this.outputQueue = outputQueue;
            this.dataBlocksPool = dataBlocksPool;
            this.byteArrayPool = byteArrayPool;
            this.compressor = compressor;
        }

        public void JobStart()
        {
            var header = new byte[compressorHeader.Length];
            inputStream.Read(header, 0, compressorHeader.Length);
            if (!header.SequenceEqual(compressorHeader))
                throw new InvalidDataException("The archive entry was compressed using an unsupported compression method");

            var blocksCountBytes = new byte[sizeof(long)];
            inputStream.Read(blocksCountBytes, 0, sizeof(long));
            var blocksCount = BitConverter.ToInt64(blocksCountBytes);

            var blockNumberBytes = new byte[sizeof(int)];
            var blockLengthBytes = new byte[sizeof(int)];
            var blockHashCodeBytes = new byte[sizeof(int)];
            for (var i = 0; i < blocksCount; i++)
            {
                inputStream.Read(blockNumberBytes, 0, blockNumberBytes.Length);
                var blockNumber = BitConverter.ToInt32(blockNumberBytes);

                inputStream.Read(blockLengthBytes, 0, blockLengthBytes.Length);
                var blockLength = BitConverter.ToInt32(blockLengthBytes);

                var data = byteArrayPool.Rent(blockLength);
                inputStream.Read(data, 0, blockLength);

                inputStream.Read(blockHashCodeBytes, 0, blockHashCodeBytes.Length);
                var blockHashCode = BitConverter.ToInt32(blockHashCodeBytes);

                var dataBlock = dataBlocksPool.Get();
                dataBlock.Data = data;
                dataBlock.Length = blockLength;
                dataBlock.Number = blockNumber;
                if (HashCalculator.GetDataBlockHashCode(dataBlock.Data, dataBlock.Length, dataBlock.Number) != blockHashCode)
                    throw new InvalidDataException("The archive entry was compressed using an unsupported compression method");

                inputQueue.Enqueue(dataBlock);
            }
            inputQueue.Finish();
        }

        public void ParallelizableJob()
        {
            while (inputQueue.Dequeue(out var dataBlock))
            {
                var compressedData = byteArrayPool.Rent(dataBlock.Length * 2);
                var compressedDataLen = compressor.Decompress(dataBlock.Data, dataBlock.Length, compressedData);
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
            var dict = new Dictionary<int, DataBlock>();
            int blockNumber = 0;

            while (outputQueue.Dequeue(out var dataBlock))
            {
                dict.Add(dataBlock.Number, dataBlock);

                if (dict.ContainsKey(blockNumber))
                {
                    var currentBlock = dict[blockNumber];
                    outputStream.Write(currentBlock.Data, 0, currentBlock.Length);
                    dict.Remove(blockNumber);
                    byteArrayPool.Return(currentBlock.Data);
                    dataBlocksPool.Return(currentBlock);
                    blockNumber++;
                }
            }

            foreach (var dataBlock in dict.OrderBy(x => x.Key).Select(x => x.Value))
            {
                outputStream.Write(dataBlock.Data, 0, dataBlock.Length);
                byteArrayPool.Return(dataBlock.Data);
                dataBlocksPool.Return(dataBlock);
            }
            outputStream.Flush();
        }

        public void Dispose()
        {
            inputQueue.Finish();
            outputQueue.Finish();
            inputStream.Close();
            outputStream.Close();
        }
    }
}