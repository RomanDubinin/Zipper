namespace GZip.Compression
{
    public interface IPartiallyParallelizableJob
    {
        void JobStart();
        void ParallelizableJob();
        void AfterAllParallelJobs();
        void JobEnd();
        void EmergencyStop();
        float GetProgress();
    }
}