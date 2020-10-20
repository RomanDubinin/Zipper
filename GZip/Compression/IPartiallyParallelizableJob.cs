﻿namespace GZip.Compression
{
    public interface IPartiallyParallelizableJob
    {
        void JobStart();
        void ParallelizableJob();
        void AfterAllParallelJobs();
        void JobEnd();
        void Dispose();
    }
}