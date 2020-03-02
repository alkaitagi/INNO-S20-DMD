using System;

namespace INNO_S20_DMD_1
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 1 && args[0] == "-m")
                Migration.Start();
        }
    }
}
