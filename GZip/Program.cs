using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
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

        private static StringBuilder progressStringBuilder = new StringBuilder(500);

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
            IPartiallyParallelizableJob job;

            if (command == compressCommand)
            {
                job = new ParallelCompressor(
                    inputStream,
                    outputStream,
                    blockSize,
                    header,
                    inputQueue,
                    outputQueue,
                    dataBlocksPool,
                    byteArrayPool);
            }
            else
            {
                var drive = new DriveInfo(Directory.GetDirectoryRoot(outputFile));
                var maxOutputFileSize = filesystemMaxFileSize.ContainsKey(drive.DriveFormat)
                    ? filesystemMaxFileSize[drive.DriveFormat]
                    : (long?)null;

                job = new ParallelDecompressor(
                    inputStream,
                    outputStream,
                    blockSize,
                    header,
                    inputQueue,
                    outputQueue,
                    dataBlocksPool,
                    byteArrayPool,
                    maxOutputFileSize);
            }
            using var timer = new Timer(x => ShowProgress($"{command}ing", job.GetProgress()), null, TimeSpan.Zero, TimeSpan.FromMilliseconds(100));
            jobRunner.RunParallel(job);

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

            ShowProgress($"{command}ing", 1);
            Console.WriteLine();
            Console.WriteLine("Done");
            return 0;
        }

        private static void ShowProgress(string command, float progress)
        {
            progressStringBuilder.Append($"{command}: {progress.ToString("P1").PadRight(6, ' ')} ");
            progressStringBuilder.Append("[");
            var progressBarLen = Console.WindowWidth - progressStringBuilder.Length - 2;
            var progressInChars = progress * progressBarLen;
            var i = 0;
            for (; i < progressInChars; i++)
                progressStringBuilder.Append("#");
            for (; i < progressBarLen; i++)
                progressStringBuilder.Append("-");
            progressStringBuilder.Append("]");
            Console.Write($"\r{progressStringBuilder}");
            progressStringBuilder.Clear();
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
