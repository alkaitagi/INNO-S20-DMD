﻿namespace INNO_S20_DMD_1
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 1 && args[0] == "-m")
                Migration.Start();
            else if (args.Length >= 2 && args[0] == "-q")
                switch (args[1])
                {
                    case "1":
                        {
                            break;
                        }
                    case "2":
                        {
                            break;
                        }
                }
        }
    }
}
