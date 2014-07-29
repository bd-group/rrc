using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using System.IO;
using System.Data;

namespace ClusterParallelLib
{
    public enum eCodeType
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

    public class Sentence
    {
        public string sentence;
        public HashSet<string> words;
    }

    public class Cluster
    {
        const string path = @".\ICTCLAS50.dll";

        // Init EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_Init", CallingConvention = CallingConvention.Cdecl)]
        public static extern bool ICTCLAS_Init(String sInitDirPath);

        // Exit EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_Exit")]
        public static extern bool ICTCLAS_Exit();

        // ParagraphProcessAW EntryPoint
        [DllImport(path, CharSet = CharSet.Ansi, EntryPoint = "ICTCLAS_ParagraphProcessAW", CallingConvention = CallingConvention.Cdecl)]//, CallingConvention = CallingConvention.Winapi)]
        public static extern int ICTCLAS_ParagraphProcessAW(String sParagraph, [Out, MarshalAs(UnmanagedType.LPArray)]result_t[] result, eCodeType eCT, int bPOSTagged);

        private static string dictPath = Path.Combine(Environment.CurrentDirectory, "Data") + Path.DirectorySeparatorChar;

        private static string testFileDir = Path.Combine(Environment.CurrentDirectory, "testFile") + Path.DirectorySeparatorChar;

        private static Dictionary<string, int> stopWordDict = new Dictionary<string, int>()
        {            
            {"呵", 1},
            {"滴", 1},
            {"-P", 1},
            {"～", 1},
        };

        private HashSet<string> inputData = new HashSet<string>();
        int count;
        float threshold;

        public int Init(string configFilePath)
        {
            return 0;
        }

        public int DeliveryData(DataTable dataTable)
        {
            return 0;
        }

        public int GetClustered(out DataTable dataTableResult)
        {
            dataTableResult = null;
            return 0;
        }

        public int CostFunction(int count, float threshold)
        {
            this.count = count;
            this.threshold = threshold;

            return 0;
        }

        public Cluster()
        {
            // 分词系统初始化
            if (!ICTCLAS_Init(null))
            {
                System.Console.WriteLine("Init ICTCLAS failed!");
                //
                System.Console.Read();
                return;
            }
        }

        public int DeliveryData(List<string> data)
        {
            if (data == null || data.Count == 0)
            {
                return -1;
            }

            inputData.Clear();
            foreach (var str in data)
            {
                inputData.Add(str);
            }

            return 0;
        }

        public int GetClustered(out List<List<Sentence>> dataResult)
        {
            try
            {
                //通过AsParallel来并行化操作,对于每个元素调用SplitWord进行分词
                List<Sentence> sentenceList = inputData.AsParallel().Select(input => SplitWord(input)).ToList();
                // 将所有sentence 分成几个大的类别
                List<List<Sentence>> classifiedSentenceCollection = ClassifySentence(sentenceList);

                // 对每个集合调用ComputeCluster，计算出每个集合对应的聚类，然后对于每个聚类，调用Aggreate聚会成一个最终的结果
                // Select 对应map操作，而aggregate对应reduce操作
                dataResult = classifiedSentenceCollection.AsParallel().
                    Select(sList => ComputeCluster(sList)).
                    Aggregate((list1, list2) =>
                    {
                        list1.AddRange(list2);
                        return list1;
                    });
            }
            catch (AggregateException ae)
            {
                dataResult = null;
                foreach (var e in ae.InnerExceptions)
                {
                    Console.WriteLine("Exceptions: {0}", e.ToString());
                }
            }

            return 0;
        }

        private Sentence SplitWord(string input)
        {
            Sentence sentence = new Sentence();
            sentence.sentence = input;
            // do word splitation here.
            var splitedResult = new result_t[input.Length];
            int wordCount = ICTCLAS_ParagraphProcessAW(input, splitedResult, eCodeType.CODE_TYPE_UNKNOWN, 1);
            byte[] inputByte = System.Text.Encoding.Default.GetBytes(input);
            sentence.words = GetSplitedWords(inputByte, splitedResult, wordCount);

            return sentence;
        }

        private HashSet<string> GetSplitedWords(byte[] inputBytes, result_t[] splitedResult, int wordCount)
        {
            HashSet<string> wordList = new HashSet<string>();
            for (int i = 0; i < wordCount; i++)
            {
                byte[] wordBytes = new byte[splitedResult[i].length];
                Array.Copy(inputBytes, splitedResult[i].start, wordBytes, 0, wordBytes.Length);
                string word = System.Text.Encoding.Default.GetString(wordBytes);
                if (string.IsNullOrWhiteSpace(word) || stopWordDict.Keys.Contains(word))
                {
                    continue;
                }

                wordList.Add(word);
            }

            return wordList;
        }

        /// <summary>
        /// do classfiy
        /// </summary>
        /// <param name="sentenceList">input sentences</param>
        /// <returns>classified sentences</returns>
        protected virtual List<List<Sentence>> ClassifySentence(List<Sentence> sentenceList)
        {
            var ret = new List<List<Sentence>>();
            List<Sentence> lengthOver10Sentences = sentenceList.Where(s => s.sentence.Length >= 10).ToList();
            List<Sentence> lengthLess10Sentences = sentenceList.Where(s => s.sentence.Length < 10).ToList();
            Console.WriteLine("count is {0} {1}", lengthLess10Sentences.Count, lengthOver10Sentences.Count);
            ret.Add(lengthLess10Sentences);
            ret.Add(lengthOver10Sentences);
            return ret;
        }

        protected virtual List<List<Sentence>> ComputeCluster(List<Sentence> sentenceList)
        {
            sentenceList = sentenceList.OrderBy(s => s.words.Count).ToList();
            var ret = new List<List<Sentence>>();
            // do computation here
            while (sentenceList.Count > 0)
            {
                List<Sentence> curCluster = new List<Sentence>();
                List<Sentence> left = new List<Sentence>();
                Sentence curRef = sentenceList[0];
                curCluster.Add(curRef);
                for (int i = 1; i < sentenceList.Count; i++)
                {
                    if (jaccardCoefficient(curRef.words, sentenceList[i].words) >= this.threshold)
                    {
                        curCluster.Add(sentenceList[i]);
                    }
                    else
                    {
                        left.Add(sentenceList[i]);
                    }
                }

                ret.Add(curCluster);
                sentenceList = left;
            }

            return ret;
        }

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

            return (float)intersectionCount / (float)(target.Count + tobeCluster.Count - intersectionCount);
            //return (float)intersectionCount / (float)tobeCluster.Count;
        }
    }
}
