using System;
using System.Collections.Generic;
using System.Threading;

namespace GZip
{
    public class BlockingQueue<T> : IDisposable
    {
        private readonly Queue<T> queue;
        private readonly int capacity;
        private bool isDisposed;
        private bool isFinished;
        private readonly object lockObject;

        public BlockingQueue(int capacity)
        {
            this.capacity = capacity;
            queue = new Queue<T>(capacity);
            isDisposed = false;
            isFinished = false;
            lockObject = new object();
        }

        public bool Enqueue(T value)
        {
            lock (lockObject)
            {
                while (!isDisposed && queue.Count >= capacity)
                    Monitor.Wait(lockObject);
                if (isDisposed)
                    return false;

                queue.Enqueue(value);
                Monitor.Pulse(lockObject);
                return true;
            }
        }

        public bool Dequeue(out T value)
        {
            value = default;
            lock (lockObject)
            {
                while (!isDisposed && !isFinished && queue.Count == 0)
                    Monitor.Wait(lockObject);
                if (isDisposed)
                    return false;
                if (isFinished && queue.Count == 0)
                    return false;

                value = queue.Dequeue();
                Monitor.Pulse(lockObject);
                return true;
            }
        }

        public void Finish()
        {
            lock (lockObject)
            {
                isFinished = true;
                Monitor.PulseAll(lockObject);
            }
        }

        public void Dispose()
        {
            lock (lockObject)
            {
                isDisposed = true;
                Monitor.PulseAll(lockObject);
            }
        }
    }
}