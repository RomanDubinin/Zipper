using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using GZip.Compression;
using GZip.Infrastructure;

namespace GZip
{
    class Program
    {
        private static readonly string logFileName = "Log.txt";
        private static readonly Type[] knownExceptions = new[] {typeof(InvalidDataException), typeof(FilesystemException)};

        private static readonly string compressCommand = "compress";
        private static readonly string decompressCommand = "decompress";
        private static readonly string[] availableCommands = {compressCommand, decompressCommand};

        private static readonly string usage = "\tUsage: " +
                                               $"{System.Reflection.Assembly.GetExecutingAssembly().GetName().Name}.exe " +
                                               $"{string.Join('/', availableCommands)} " +
                                               "input_file_name " +
                                               "output_file_name";

        private static readonly string[] doReplaceCommands = {"Y", "y"};
        private static readonly string[] doNotReplaceCommands = { "N", "n" };

        private static readonly Dictionary<string, long> filesystemMaxFileSize = new Dictionary<string, long>()
        {
            {"FAT32", (long) 4 * 1024 * 1024 * 1024 - 1}
        };

        private static readonly int coresNumber = Environment.ProcessorCount;
        private static readonly int blockSize = 1024 * 1024;
        private static readonly byte[] header = { 71, 90, 105, 112, 84, 101, 115, 116 };

        static int Main(string[] args)
        {
            var (error, command, inputFile, outputFile) = ParseCommandLineArguments(args);

            if (error != null)
            {
                Console.WriteLine(error);
                return 1;
            }

            var directory = Path.GetDirectoryName(outputFile);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            var inputStream = new FileStream(inputFile, FileMode.Open);
            var outputStream = new FileStream(outputFile, FileMode.Create);

            var inputQueue = new BlockingQueue<DataBlock>(coresNumber);
            var outputQueue = new BlockingQueue<DataBlock>(coresNumber);

            var dataBlocksPool = new ObjectPool<DataBlock>(() => new DataBlock());
            var byteArrayPool = ArrayPool<byte>.Create(blockSize * 2, coresNumber * 2);

            var threadRunner = new ThreadRunner();
            var jobRunner = new JobRunner(coresNumber, threadRunner);

            if (command == compressCommand)
            {
                var parallelCompressor = new ParallelCompressor(
                    inputStream,
                    outputStream,
                    blockSize,
                    header,
                    inputQueue,
                    outputQueue,
                    dataBlocksPool,
                    byteArrayPool);
                jobRunner.RunParallel(parallelCompressor);
            }

            if (command == decompressCommand)
            {
                var drive = new DriveInfo(Directory.GetDirectoryRoot(outputFile));
                var maxOutputFileSize = filesystemMaxFileSize.ContainsKey(drive.DriveFormat)
                    ? filesystemMaxFileSize[drive.DriveFormat]
                    : (long?)null;

                var parallelDecompressor = new ParallelDecompressor(
                    inputStream,
                    outputStream,
                    blockSize,
                    header,
                    inputQueue,
                    outputQueue,
                    dataBlocksPool,
                    byteArrayPool,
                    maxOutputFileSize);
                jobRunner.RunParallel(parallelDecompressor);
            }

            inputStream.Close();
            outputStream.Close();

            var exceptions = jobRunner.GetRaisedExceptions();
            if (exceptions.Any())
            {
                File.Delete(outputFile);
                WriteExceptionToLog(exceptions);
                PrintErrorMessage(exceptions);
                return 1;
            }

            Console.WriteLine("Done");
            return 0;
        }

        private static void PrintErrorMessage(List<Exception> exceptions)
        {
            var exceptionsToPrint = exceptions
                .Where(a => knownExceptions.Contains(a.GetType()))
                .Select(x => x.Message)
                .ToArray();
            var messageToPrint = exceptionsToPrint.Any()
                ? $"\n\tOperation was failed:\n{string.Join('\n', exceptionsToPrint)}."
                : $"\n\tOperation was failed\nPlease contact the author and send him {logFileName} file";
            Console.WriteLine(messageToPrint);
        }

        private static void WriteExceptionToLog(List<Exception> exceptions)
        {
            File.Delete(logFileName);
            foreach (var exception in exceptions)
            {
                File.AppendAllText(logFileName, exception.GetType() + "\n");
                File.AppendAllText(logFileName, exception.Message + "\n");
                File.AppendAllText(logFileName, exception.Source + "\n");
                File.AppendAllText(logFileName, exception.StackTrace + "\n");
                File.AppendAllText(logFileName, "\n");
            }
        }

        private static (string error, string command, string inputFile, string outputFile) ParseCommandLineArguments(string[] args)
        {
            if (args.Length != 3)
                return (usage, null, null, null);

            var command = args[0];
            var inputFile = args[1];
            var outputFile = args[2];

            if (!availableCommands.Contains(command))
                return (usage, null, null, null);

            if (!File.Exists(inputFile))
                return ("\tThe input file does not exists.", null, null, null);

            if (File.Exists(outputFile))
            {
                Console.WriteLine("\tThe output file already exists.");
                Console.Write("Do you want to replace it ");
                var replaceCommand = "";
                while (!doReplaceCommands.Contains(replaceCommand) && !doNotReplaceCommands.Contains(replaceCommand))
                {
                    Console.Write("(Y/N)? ");
                    replaceCommand = Console.ReadLine();
                }

                if (doNotReplaceCommands.Contains(replaceCommand))
                {
                    return ("", null, null, null);
                }
            }

            return (null, command, inputFile, outputFile);
        }
    }
}
