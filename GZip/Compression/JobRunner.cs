﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using GZip.Infrastructure;

namespace GZip.Compression
{
    public class JobRunner
    {
        private readonly int compressorsNumber;
        private readonly ThreadRunner threadRunner;

        public JobRunner(int compressorsNumber, 
            ThreadRunner threadRunner)
        {
            this.compressorsNumber = compressorsNumber;
            this.threadRunner = threadRunner;
        }

        public void RunParallel(IPartiallyParallelizableJob job)
        {
            var readThread = threadRunner.RunWithExceptionHandling(job.JobStart, job.EmergencyStop);
            var compressors = new Thread[compressorsNumber];
            for (var i = 0; i < compressorsNumber; i++)
            {
                compressors[i] = threadRunner.RunWithExceptionHandling(job.ParallelizableJob, job.EmergencyStop);
            }
            var writeThread = threadRunner.RunWithExceptionHandling(job.JobEnd, job.EmergencyStop);

            readThread.Start();
            for (var i = 0; i < compressorsNumber; i++)
            {
                compressors[i].Start();
            }
            writeThread.Start();

            readThread.Join();
            for (var i = 0; i < compressorsNumber; i++)
            {
                compressors[i].Join();
            }
            job.AfterAllParallelJobs();
            writeThread.Join();
        }

        public List<Exception> GetRaisedExceptions()
        {
            return threadRunner.Exceptions.ToList();
        }
    }
}