using System;
using System.Collections.Concurrent;
using System.Threading;

namespace GZip
{
    public class ThreadRunner
    {
        public readonly ConcurrentBag<Exception> Exceptions;

        public ThreadRunner()
        {
            Exceptions = new ConcurrentBag<Exception>();
        }

        public Thread RunWithExceptionHandling(ThreadStart action)
        {
            return new Thread(() =>
            {
                try
                {
                    action();
                }
                catch (Exception e)
                {
                    Exceptions.Add(e);
                }
            });
        }
    }
}