﻿using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace GZip
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
            var blocksCount = inputStream.Length / blockSize +
                              (inputStream.Length % blockSize == 0 ? 0 : 1);
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
                var compressedData = byteArrayPool.Rent(dataBlock.Length * 2);
                var compressedDataLen = compressor.Compress(dataBlock.Data, dataBlock.Length, compressedData);
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
            var blocksCount = inputStream.Length / blockSize +
                              (inputStream.Length % blockSize == 0 ? 0 : 1);

            outputStream.Write(compressorHeader);
            outputStream.Write(BitConverter.GetBytes(blocksCount));

            while (outputQueue.Dequeue(out var dataBlock))
            {
                dict.Add(dataBlock.Number, dataBlock);

                //todo remove order
                if (dict.ContainsKey(blockNumber))
                {
                    var currentBlock = dict[blockNumber];
                    outputStream.Write(BitConverter.GetBytes(currentBlock.Number));
                    outputStream.Write(BitConverter.GetBytes(currentBlock.Length));
                    outputStream.Write(currentBlock.Data, 0, currentBlock.Length);
                    outputStream.Write(BitConverter.GetBytes(HashCalculator.GetDataBlockHashCode(currentBlock.Data, currentBlock.Length, currentBlock.Number)));
                    dict.Remove(blockNumber);
                    byteArrayPool.Return(currentBlock.Data);
                    dataBlocksPool.Return(currentBlock);
                    blockNumber++;
                }
            }

            foreach (var dataBlock in dict.OrderBy(x => x.Key).Select(x => x.Value))
            {
                outputStream.Write(BitConverter.GetBytes(dataBlock.Number));
                outputStream.Write(BitConverter.GetBytes(dataBlock.Length));
                outputStream.Write(dataBlock.Data, 0, dataBlock.Length);
                byteArrayPool.Return(dataBlock.Data);
                outputStream.Write(BitConverter.GetBytes(HashCalculator.GetDataBlockHashCode(dataBlock.Data, dataBlock.Length, dataBlock.Number)));
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