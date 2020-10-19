using System;
using System.Collections.Concurrent;
using System.Threading;

namespace GZip.Infrastructure
{
    public class ThreadRunner
    {
        public readonly ConcurrentBag<Exception> Exceptions;

        public ThreadRunner()
        {
            Exceptions = new ConcurrentBag<Exception>();
        }

        public Thread RunWithExceptionHandling(ThreadStart action, Action onException)
        {
            return new Thread(() =>
            {
                try
                {
                    action();
                }
                catch (Exception e)
                {
                    onException();
                    Exceptions.Add(e);
                }
            });
        }
    }
}