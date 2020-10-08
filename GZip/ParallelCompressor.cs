using System;
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
            var readThread = new Thread(Read);
            var compressors = new Thread[compressorsNumber];
            for (int i = 0; i < compressorsNumber; i++)
            {
                compressors[i] = new Thread(DoCompress);
            }
            var writeThread = new Thread(Write);

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

        private void Read()
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

        private void Write()
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
    }
}