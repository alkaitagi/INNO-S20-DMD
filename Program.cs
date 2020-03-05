using System;
using System.Diagnostics;

namespace INNO_S20_DMD_1
{
    public class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0 || args.Length == 1 && args[0] == "-m")
                Migration.Start();
            else if (args.Length >= 2 && args[0] == "-q")
            {
                Console.WriteLine($"Executing query {args[1]}...");
                Process.Start("cmd.exe", $"/C python queries/{args[1]}.py");
            }
        }
    }
}
