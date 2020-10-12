using System;
using System.Collections.Concurrent;
using System.Threading;

namespace GZip
{
    public class ThreadRunner
    {
        public readonly ConcurrentBag<Exception> Exceptions;

        private readonly Action onException;

        public ThreadRunner(Action onException)
        {
            this.onException = onException;
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
                    onException();
                    Exceptions.Add(e);
                }
            });
        }
    }
}