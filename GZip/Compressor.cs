﻿using System;
using System.IO;
using System.IO.Compression;

namespace GZip
{
    public class Compressor
    {
        [Obsolete]
        public byte[] Compress(byte[] data, int length)
        {
            using var compressedStream = new MemoryStream();
            using (var compressionStream = new GZipStream(compressedStream, CompressionMode.Compress))
                compressionStream.Write(data, 0, length);

            return compressedStream.ToArray();
        }

        //todo if decompress bytes that have not been compressed - InvalidDataException
        [Obsolete]
        public byte[] Decompress(byte[] data, int length)
        {
            using var originalStream = new MemoryStream(data, 0, length);
            using var gzipStream = new GZipStream(originalStream, CompressionMode.Decompress);
            using var decompressedStream = new MemoryStream();
            gzipStream.CopyTo(decompressedStream);

            return decompressedStream.ToArray();
        }

        public int Compress(byte[] data, int length, byte[] compressedData)
        {
            using var compressedStream = new MemoryStream();
            using var compressionStream = new GZipStream(compressedStream, CompressionMode.Compress, true);
            compressionStream.Write(data, 0, length);
            compressionStream.Flush();
            compressedStream.Seek(0, SeekOrigin.Begin);
            compressedStream.Read(compressedData, 0, (int)compressedStream.Length);
            return (int)compressedStream.Length;
        }

        public int Decompress(byte[] data, int length, byte[] decompressedData)
        {
            using var originalStream = new MemoryStream(data, 0, length);
            using var gzipStream = new GZipStream(originalStream, CompressionMode.Decompress);
            using var decompressedStream = new MemoryStream();
            gzipStream.CopyTo(decompressedStream);

            decompressedStream.Seek(0, SeekOrigin.Begin);
            decompressedStream.Read(decompressedData, 0, (int)decompressedStream.Length);
            return (int) decompressedStream.Length;
        }

    }
}