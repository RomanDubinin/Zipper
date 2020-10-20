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
        private readonly int originalBlockSize;
        private readonly Stream inputStream;
        private readonly Stream outputStream;

        private readonly BlockingQueue<DataBlock> inputQueue;
        private readonly BlockingQueue<DataBlock> outputQueue;

        private readonly ObjectPool<DataBlock> dataBlocksPool;
        private readonly ArrayPool<byte> byteArrayPool;

        private readonly Compressor compressor;

        private readonly byte[] compressorHeader;

        private readonly byte[] longBuffer;
        private readonly byte[] intBuffer;

        private readonly long? filesystemMaximumFileSize;

        public ParallelDecompressor(Stream inputStream,
                                    Stream outputStream,
                                    int originalBlockSize,
                                    byte[] compressorHeader,
                                    BlockingQueue<DataBlock> inputQueue,
                                    BlockingQueue<DataBlock> outputQueue,
                                    ObjectPool<DataBlock> dataBlocksPool,
                                    ArrayPool<byte> byteArrayPool,
                                    Compressor compressor,
                                    long? filesystemMaximumFileSize = null)
        {
            this.originalBlockSize = originalBlockSize;
            this.inputStream = inputStream;
            this.outputStream = outputStream;
            this.compressorHeader = compressorHeader;
            this.inputQueue = inputQueue;
            this.outputQueue = outputQueue;
            this.dataBlocksPool = dataBlocksPool;
            this.byteArrayPool = byteArrayPool;
            this.compressor = compressor;
            this.filesystemMaximumFileSize = filesystemMaximumFileSize;

            longBuffer = new byte[sizeof(long)];
            intBuffer = new byte[sizeof(int)];
        }

        public void JobStart()
        {
            var header = new byte[compressorHeader.Length];
            inputStream.Read(header, 0, compressorHeader.Length);
            if (!header.SequenceEqual(compressorHeader))
                throw new InvalidDataException("The archive entry was compressed using an unsupported compression method");

            var originalFileSize = ReadLong(inputStream);
            if (originalFileSize > filesystemMaximumFileSize)
            {
                throw new FilesystemException("File system does not supports files of such size.");
            }

            var blocksCount = ReadLong(inputStream);
            for (var i = 0; i < blocksCount; i++)
            {
                var blockNumber = ReadInt(inputStream);
                var blockLength = ReadInt(inputStream);

                var data = byteArrayPool.Rent(blockLength);
                inputStream.Read(data, 0, blockLength);

                var blockHashCode = ReadInt(inputStream);

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
                var decompressedData = byteArrayPool.Rent(originalBlockSize);
                var compressedDataLen = compressor.Decompress(dataBlock.Data, dataBlock.Length, decompressedData);
                byteArrayPool.Return(dataBlock.Data);
                dataBlock.Data = decompressedData;
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

        private int ReadInt(Stream stream)
        {
            stream.Read(intBuffer, 0, intBuffer.Length);
            return BitConverter.ToInt32(intBuffer);
        }

        private long ReadLong(Stream stream)
        {
            stream.Read(longBuffer, 0, longBuffer.Length);
            return BitConverter.ToInt64(longBuffer);
        }
    }
}