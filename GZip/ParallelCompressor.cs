using System;
using System.Buffers;
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

        private readonly Stream inputStream;
        private readonly Stream outputStream;

        private readonly BlockingQueue<DataBlock> inputQueue;
        private readonly BlockingQueue<DataBlock> outputQueue;

        private readonly ObjectPool<DataBlock> dataBlocksPool;
        private readonly ArrayPool<byte> byteArrayPool;

        private readonly Compressor compressor;

        private readonly byte[] compressorHeader;

        private readonly ThreadRunner threadRunner;

        public ParallelCompressor(int compressorsNumber, int blockSize, Stream inputStream, Stream outputStream, byte[] compressorHeader)
        {
            this.compressorsNumber = compressorsNumber;
            this.blockSize = blockSize;

            this.inputStream = inputStream;
            this.outputStream = outputStream;
            this.compressorHeader = compressorHeader;

            inputQueue = new BlockingQueue<DataBlock>(compressorsNumber);
            outputQueue = new BlockingQueue<DataBlock>(compressorsNumber);

            dataBlocksPool = new ObjectPool<DataBlock>(() => new DataBlock());
            byteArrayPool = ArrayPool<byte>.Create(blockSize * 2, compressorsNumber * 2);

            compressor = new Compressor();

            threadRunner = new ThreadRunner();
        }

        public void CompressParallel()
        {
            DoParallel(ReadUncompressed, DoCompress, WriteCompressed);
        }

        public void DecompressParallel()
        {
            DoParallel(ReadCompressed, DoDecompress, WriteDecompressed);
        }

        public List<Exception> GetRaisedExceptions()
        {
            return threadRunner.Exceptions.ToList();
        }

        private void DoParallel(ThreadStart read, ThreadStart doJob, ThreadStart write)
        {
            var readThread = threadRunner.RunWithExceptionHandling(read, OnException);
            var compressors = new Thread[compressorsNumber];
            for (var i = 0; i < compressorsNumber; i++)
            {
                compressors[i] = threadRunner.RunWithExceptionHandling(doJob, OnException);
            }
            var writeThread = threadRunner.RunWithExceptionHandling(write, OnException);

            readThread.Start();
            for (var i = 0; i < compressorsNumber; i++)
            {
                compressors[i].Start();
            }
            writeThread.Start();

            readThread.Join();
            for (var i = 0; i < compressorsNumber; i++)
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

        private void DoCompress()
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

        private void DoDecompress()
        {
            while (inputQueue.Dequeue(out var dataBlock))
            {
                var decompressedData = byteArrayPool.Rent(blockSize);
                var decompressedDataLen = compressor.Decompress(dataBlock.Data, dataBlock.Length, decompressedData);
                byteArrayPool.Return(dataBlock.Data);
                dataBlock.Data = decompressedData;
                dataBlock.Length = decompressedDataLen;
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

        private void OnException()
        {
            inputQueue.Finish();
            outputQueue.Finish();
            inputStream.Close();
            outputStream.Close();
        }
    }
}