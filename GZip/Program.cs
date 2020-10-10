using System;
using System.IO;
using System.Linq;

namespace GZip
{
    class Program
    {
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

        private static readonly int coresNumber = Environment.ProcessorCount;
        private static readonly int blockSize = 1024 * 1024;
        private static readonly byte[] header = { 7, 7, 7 };

        static int Main(string[] args)
        {
            var (error, command, inputFile, outputFile) = ParseCommandLineArguments(args);

            if (error != null)
            {
                Console.WriteLine(error);
                return 1;
            }

            var inputStream = new FileStream(inputFile, FileMode.Open);
            var outputStream = new FileStream(outputFile, FileMode.Create);

            var compressor = new ParallelCompressor(coresNumber, blockSize, inputStream, outputStream, header);

            if (command == compressCommand)
                compressor.CompressParallel();
            if (command == decompressCommand)
                compressor.DecompressParallel();

            Console.WriteLine("Done");
            return 0;
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
