namespace GZip.Infrastructure
{
    public static class HashCalculator
    {
        public static int GetDataBlockHashCode(byte[] data, int length, int number)
        {
            int hash = 17;
            unchecked
            {
                for (var i = 0; i < length; i++)
                {
                    hash = hash * 31 + data[i].GetHashCode();
                }
            }
            return ((hash * 31 + length) * 31 + number) * 31;
        }
    }
}