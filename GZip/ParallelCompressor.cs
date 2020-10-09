﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace GZip
{
    public class ParallelCompressor
    {
        private readonly int compressorsNumber;
        private readonly int blockSize;

        private readonly MemoryStream inputStream;
        private readonly MemoryStream outputStream;

        private readonly BlockingQueue<DataBlock> inputQueue;
        private readonly BlockingQueue<DataBlock> outputQueue;

        private readonly Compressor compressor;

        private readonly byte[] compressorHeader;

        public ParallelCompressor(int compressorsNumber, int blockSize, MemoryStream inputStream, MemoryStream outputStream, byte[] compressorHeader)
        {
            this.compressorsNumber = compressorsNumber;
            this.blockSize = blockSize;

            this.inputStream = inputStream;
            this.outputStream = outputStream;
            this.compressorHeader = compressorHeader;

            inputQueue = new BlockingQueue<DataBlock>(compressorsNumber);
            outputQueue = new BlockingQueue<DataBlock>(compressorsNumber);

            compressor = new Compressor();
        }

        public void CompressParallel()
        {
            DoParallel(ReadUncompressed, DoCompress, WriteCompressed);
        }

        public void DecompressParallel()
        {
            DoParallel(ReadCompressed, DoDecompress, WriteDecompressed);
        }

        private void DoParallel(ThreadStart read, ThreadStart doJob, ThreadStart write)
        {
            var readThread = new Thread(read);
            var compressors = new Thread[compressorsNumber];
            for (int i = 0; i < compressorsNumber; i++)
            {
                compressors[i] = new Thread(doJob);
            }
            var writeThread = new Thread(write);

            readThread.Start();
            for (int i = 0; i < compressorsNumber; i++)
            {
                compressors[i].Start();
            }
            writeThread.Start();

            readThread.Join();
            for (int i = 0; i < compressorsNumber; i++)
            {
                compressors[i].Join();
            }
            outputQueue.Finish();
            writeThread.Join();
        }

        private void ReadUncompressed()
        {
            var blocksCount = inputStream.Length / blockSize +
                              (inputStream.Length % blockSize == 0 ? 0 : 1);
            for (var i = 0; i < blocksCount; i++)
            {
                //todo pool of objects
                var data = new byte[blockSize];
                var blockLen = inputStream.Read(data, 0, blockSize);

                var dataBlock = new DataBlock {Data = data, Length = blockLen, Number = i};
                inputQueue.Enqueue(dataBlock);
            }
            inputQueue.Finish();
        }

        private void ReadCompressed()
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
            for (var i = 0; i < blocksCount; i++)
            {
                inputStream.Read(blockNumberBytes, 0, blockNumberBytes.Length);
                inputStream.Read(blockLengthBytes, 0, blockLengthBytes.Length);
                var blockNumber = BitConverter.ToInt32(blockNumberBytes);
                var blockLength = BitConverter.ToInt32(blockLengthBytes);
                //todo pool of objects
                var data = new byte[blockLength];
                inputStream.Read(data, 0, blockLength);

                var dataBlock = new DataBlock {Data = data, Length = blockLength, Number = blockNumber};
                inputQueue.Enqueue(dataBlock);
            }
            inputQueue.Finish();
        }

        private void DoCompress()
        {
            while (inputQueue.Dequeue(out var dataBlock))
            {
                var compressedData = compressor.Compress(dataBlock.Data, dataBlock.Length);
                dataBlock.Data = compressedData;
                dataBlock.Length = compressedData.Length;
                if (!outputQueue.Enqueue(dataBlock))
                    return;
            }
        }

        private void DoDecompress()
        {
            while (inputQueue.Dequeue(out var dataBlock))
            {
                var decompressedData = compressor.Decompress(dataBlock.Data, dataBlock.Length);
                dataBlock.Data = decompressedData;
                dataBlock.Length = decompressedData.Length;
                if (!outputQueue.Enqueue(dataBlock))
                    return;
            }
        }

        private void WriteCompressed()
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
                    dict.Remove(blockNumber);
                    blockNumber++;
                }
            }

            foreach (var dataBlock in dict.OrderBy(x => x.Key).Select(x => x.Value))
            {
                outputStream.Write(BitConverter.GetBytes(dataBlock.Number));
                outputStream.Write(BitConverter.GetBytes(dataBlock.Length));
                outputStream.Write(dataBlock.Data, 0, dataBlock.Length);
            }
            outputStream.Flush();
        }

        private void WriteDecompressed()
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
                    blockNumber++;
                }
            }

            foreach (var dataBlock in dict.OrderBy(x => x.Key).Select(x => x.Value))
            {
                outputStream.Write(dataBlock.Data, 0, dataBlock.Length);
            }
            outputStream.Flush();
        }
    }
}