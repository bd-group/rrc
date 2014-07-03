using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.IO;
using System.Collections;
using System.Diagnostics;

namespace ClusterSort
{
    enum eCodeType
    {
        CODE_TYPE_UNKNOWN, //type unknown
        CODE_TYPE_ASCII, //ASCII
        CODE_TYPE_GB, //GB2312,GBK,GB10380
        CODE_TYPE_UTF8, //UTF-8
        CODE_TYPE_BIG5 //BIG5
    }
    [StructLayout(LayoutKind.Explicit)]

    // 处理结果类型定义
    public struct result_t
    {
        [FieldOffset(0)]
        public int start;
        [FieldOffset(4)]
        public int length;
        [FieldOffset(8)]
        public int sPos;
        [FieldOffset(12)]
        public int sPosLow;
        [FieldOffset(16)]
        public int POS_id;
        [FieldOffset(20)]
        public int word_ID;
        [FieldOffset(24)]
        public int word_type;
        [FieldOffset(28)]
        public int weight;
    }

    class JaccardCluster
    {
        //
        const string path = @"ICTCLAS50.dll";

        // Init EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_Init", CallingConvention = CallingConvention.Cdecl)]
        public static extern bool ICTCLAS_Init(String sInitDirPath);

        // Exit EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_Exit")]
        public static extern bool ICTCLAS_Exit();

        // ParagraphProcessAW EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_ParagraphProcessAW", CallingConvention = CallingConvention.Cdecl)]//, CallingConvention = CallingConvention.Winapi)]
        public static extern int ICTCLAS_ParagraphProcessAW(String sParagraph, [Out, MarshalAs(UnmanagedType.LPArray)]result_t[] result, eCodeType eCT, int bPOSTagged);

        // FileProcess EntryPoint 文件分词
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_FileProcess")]
        public static extern double ICTCLAS_FileProcess(String sSrcFilename, eCodeType eCt, String sDsnFilename, int bPOStagged);

        // ParagraphProcess EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_ParagraphProcess")]
        public static extern int ICTCLAS_ParagraphProcess(String sParagraph, int nPaLen, String sResult, eCodeType eCt, int bPOStagged);

        // ImportUsetDict EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_ImportUserDict")]
        public static extern int ICTCLAS_ImportUserDict(String sFilename, eCodeType eCT);

        // SetPOSmap EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_SetPOSmap")]
        public static extern int ICTCLAS_SetPOSmap(int nPOSmap);

        //
        private static string dictPath = Path.Combine(Environment.CurrentDirectory, "Data") + Path.DirectorySeparatorChar;
        //
        private static string testFileDir = Path.Combine(Environment.CurrentDirectory, "testFile") + Path.DirectorySeparatorChar;
        //
        private static string outDir = Path.Combine(Environment.CurrentDirectory, "output") + Path.DirectorySeparatorChar;
        //
        private static string stopWordDir = Path.Combine(Environment.CurrentDirectory, "stopWord") + Path.DirectorySeparatorChar;

        //
        private static List<string> fileList = new List<string>();


        public ArrayList ToCluster(StreamReader inputStream, float threshold)
        {
            //Stopwatch toClusterWatch = new Stopwatch();
            //toClusterWatch.Start();
            //
            ArrayList resultClusterArrayList = new ArrayList();
            //
            ArrayList resultArrayList = new ArrayList();
            //
            if (SplitWord(inputStream, resultArrayList))
            {
                //
                int arrayCount = resultArrayList.Count;

                
                resultArrayList.Sort(new myHashsetCompare());
                //
                int iCur = 0;

                //
                while ((arrayCount != 0) && (iCur < arrayCount))
                {
                    //
                    ArrayList aCluster = new ArrayList();
                    //

                    //
                    HashSet<string> aSet = (HashSet<string>)resultArrayList[iCur];
                    
                    //
                    int jCur = iCur + 1;
                    //
                    while (jCur < arrayCount)
                    {
                        
                        //
                        HashSet<string> bSet = (HashSet<string>)resultArrayList[jCur];

                        //
                        if (aSet.Count < threshold * bSet.Count)
                        {
                            break;
                        }

                        //
                        if (jaccardCoefficient(aSet, bSet) >= threshold)
                        {
                            aCluster.Add(bSet);
                            //
                            resultArrayList.RemoveAt(jCur);
                            //
                            arrayCount = resultArrayList.Count;
                        }
                        else
                        {
                            jCur++;
                        }
                        
                    }

                    //
                    aCluster.Add(aSet);
                    //
                    resultArrayList.RemoveAt(iCur);
                    //
                    arrayCount = resultArrayList.Count;
                    //
                    resultClusterArrayList.Add(aCluster);


                }

            }
            //
            //toClusterWatch.Stop();
            //System.Console.WriteLine("Jaccard coefficient 用时：" + toClusterWatch.ElapsedMilliseconds + " ms");

            return resultClusterArrayList;

        }


        

        /// <summary>
        /// 将流进行分词，结果写入ArrayList，每条值是一个HashSet
        /// </summary>
        /// <param name="toSplitStream"></param>
        /// <param name="splitedTermsArrayList"></param>
        /// <returns></returns>
        public bool SplitWord(StreamReader toSplitStream, ArrayList splitedTermsArrayList)
        {
            //
            Stopwatch splitWatch = new Stopwatch();
            splitWatch.Start();

            // 分词系统初始化
            if (!ICTCLAS_Init(null))
            {
                System.Console.WriteLine("Init ICTCLAS failed!");
                //
                System.Console.Read();
                return false;
            }

            // stop word
            //Hashtable stopWord = initStopWordTable();
            Hashtable stopWord = new Hashtable();
            initStopWordTable(stopWord);

            //
            splitedTermsArrayList.Clear();
            //
            result_t[] result;
            //
            int nWordCount;

            //
            string input = "";
            //
            input = toSplitStream.ReadLine();
            //
            while (input != null)
            {
                if (input == "")
                {
                    input = toSplitStream.ReadLine();
                    continue;
                }
                //
                try
                {
                    result = new result_t[input.Length];
                    //
                    nWordCount = ICTCLAS_ParagraphProcessAW(input, result, eCodeType.CODE_TYPE_UNKNOWN, 1);

                }
                catch (System.Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    //
                    continue;
                }

                // 取字符串真实长度
                byte[] mybyte = System.Text.Encoding.Default.GetBytes(input);
                //
                byte[] byteWord = new byte[1];

                //
                HashSet<string> splitedTermsSet = new HashSet<string>();
                //
                for (int j = 0; j < nWordCount; ++j)
                {
                    try
                    {
                        byteWord = new byte[result[j].length];
                    }
                    catch (System.Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    //
                    Array.Copy(mybyte, result[j].start, byteWord, 0, result[j].length);
                    //
                    string watch = System.Text.Encoding.Default.GetString(byteWord);
                    //
                    if (watch == " ")
                    {
                        continue;
                    }
                    else if (stopWord.Contains(watch))
                    {
                        continue;
                    }
                    //
                    splitedTermsSet.Add(System.Text.Encoding.Default.GetString(byteWord));
                }
                //
                if (splitedTermsSet.Count > 0)
                {
                    splitedTermsArrayList.Add(splitedTermsSet);
                }
                
                //
                input = toSplitStream.ReadLine();

            }
            //
            ICTCLAS_Exit();

            //
            splitWatch.Stop();
            System.Console.WriteLine("分词所用时间：" + splitWatch.ElapsedMilliseconds + " ms.");

            return true;
        }


        /// <summary>
        /// initialization stop word hash table
        /// </summary>
        /// <param name="stopHT"></param>
        private static void initStopWordTable(Hashtable stopHT)
        {
            stopHT.Add(",", 1);
            stopHT.Add(".", 1);
            stopHT.Add(":", 1);
            stopHT.Add("'", 1);
            stopHT.Add("\\", 1);
            stopHT.Add("-", 1);
            stopHT.Add("+", 1);
            stopHT.Add("=", 1);
            stopHT.Add("_", 1);
            stopHT.Add("?", 1);
            stopHT.Add("/", 1);
            stopHT.Add(";", 1);
            stopHT.Add("\"", 1);
            stopHT.Add("<", 1);
            stopHT.Add(">", 1);
            stopHT.Add("`", 1);
            stopHT.Add("~", 1);
            stopHT.Add("1", 1);
            stopHT.Add("!", 1);
            stopHT.Add("2", 1);
            stopHT.Add("@", 1);
            stopHT.Add("3", 1);
            stopHT.Add("#", 1);
            stopHT.Add("4", 1);
            stopHT.Add("$", 1);
            stopHT.Add("5", 1);
            stopHT.Add("%", 1);
            stopHT.Add("6", 1);
            stopHT.Add("^", 1);
            stopHT.Add("7", 1);
            stopHT.Add("&", 1);
            stopHT.Add("8", 1);
            stopHT.Add("*", 1);
            stopHT.Add("9", 1);
            stopHT.Add("(", 1);
            stopHT.Add("0", 1);
            stopHT.Add(")", 1);
            stopHT.Add("[", 1);
            stopHT.Add("]", 1);
            stopHT.Add("{", 1);
            stopHT.Add("}", 1);
            stopHT.Add("|", 1);
            // Chinese
            stopHT.Add("，", 1);
            stopHT.Add("。", 1);
            stopHT.Add("：", 1);
            stopHT.Add("；", 1);
            stopHT.Add("“", 1);
            stopHT.Add("”", 1);
            stopHT.Add("【", 1);
            stopHT.Add("】", 1);
            stopHT.Add("￥", 1);
            stopHT.Add("Ω", 1);
            stopHT.Add("？", 1);
            stopHT.Add("、", 1);
            stopHT.Add("…", 1);
            stopHT.Add("！", 1);
            //stopHT.Add("~", 1);

            stopHT.Add("的", 1);
            stopHT.Add("是", 1);
            stopHT.Add("我", 1);
            stopHT.Add("我们", 1);
            stopHT.Add("他", 1);
            stopHT.Add("她", 1);
            stopHT.Add("他们", 1);
            stopHT.Add("她们", 1);
            stopHT.Add("它", 1);
            stopHT.Add("它们", 1);
            stopHT.Add("你", 1);
            stopHT.Add("你们", 1);
            stopHT.Add("嗯", 1);
            stopHT.Add("恩", 1);
            stopHT.Add("哦", 1);
            stopHT.Add("噢", 1);
            stopHT.Add("喔", 1);
            stopHT.Add("呢", 1);
            stopHT.Add("在", 1);
            stopHT.Add("吧", 1);
            stopHT.Add("啊", 1);
            stopHT.Add("了", 1);
            stopHT.Add("啦", 1);
            stopHT.Add("哈", 1);
            stopHT.Add("呵", 1);
            stopHT.Add("滴", 1);
            stopHT.Add("-P", 1);
            stopHT.Add("～", 1);


        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="tobeCluster"></param>
        /// <returns></returns>
        private float jaccardCoefficient(HashSet<string> target, HashSet<string> tobeCluster)
        {
            //
            int intersectionCount = 0;
            //
            foreach (string s in target)
            {
                if (tobeCluster.Contains(s))
                {
                    intersectionCount++;
                }
            }

            //return (float)intersectionCount / (float)(target.Count + tobeCluster.Count - intersectionCount);
            return (float)intersectionCount / (float)tobeCluster.Count;
        }


    }

    public class myHashsetCompare:System.Collections.IComparer
    {
        public int Compare(Object x, Object y)
        {

            return ((HashSet<string>)x).Count - ((HashSet<string>)y).Count;
        }
    }


    class ExecuteJaccardCluster
    {
        static void Main(string[] args)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();
            //
            JaccardCluster jaccardCluster = new JaccardCluster();
            //

            //jaccardCluster.SplitWord();
            StreamReader sr = new StreamReader("C:\\Users\\Administrator\\Documents\\Visual Studio 2010\\Projects\\ClusterSort\\ClusterSort\\bin\\Debug\\testFile\\a.txt", System.Text.Encoding.Default);

            ArrayList resultArrayList = jaccardCluster.ToCluster(sr, (float)0.8);
            watch.Stop();
            Console.WriteLine("总用时: " + watch.ElapsedMilliseconds + " ms");
            Console.ReadKey();


        }

    }
}
