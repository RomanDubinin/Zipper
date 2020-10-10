using System;
using System.Collections.Concurrent;

namespace GZip
{
    public class ObjectPool<T>
    {
        private readonly ConcurrentBag<T> objects;
        private readonly Func<T> objectGenerator;

        public ObjectPool(Func<T> objectGenerator)
        {
            this.objectGenerator = objectGenerator ?? throw new ArgumentNullException(nameof(objectGenerator));
            objects = new ConcurrentBag<T>();
        }

        public T Get() => objects.TryTake(out T item) ? item : objectGenerator();

        public void Return(T item) => objects.Add(item);
    }
}