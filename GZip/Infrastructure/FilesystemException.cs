using System;

namespace GZip.Infrastructure
{
    public class FilesystemException : Exception
    {
        public FilesystemException(string message) : base(message) { }
    }
}