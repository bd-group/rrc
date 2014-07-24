using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.IO;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Data;

namespace ClusterLibrary
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
        public int id;
        public HashSet<string> words;
        public System.Int32 hashCode;
        //public List<int> sameId;
    }

    public class ClusterDll : ErrorCode
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

        //
        private string filePath;
        //
        private string dictDir;
        //
        private int maxThreadCount = 50;
        //
        private Hashtable stopWrodHashTable;
        //
        private float threshold = 0.5F;
        //        
        private BlockingCollection<List<Sentence>> mesgBlockCollection = new BlockingCollection<List<Sentence>>();
        //
        private DataTable resultDataTable = new DataTable("ClusterResult");
        //
       


        public int Init(string configFilePath)
        {
            this.filePath = configFilePath;

            if (this.LoadConfigFile(configFilePath) == -1)
            {
                return ErrorCode.INIT_LOAD_CONFIG_FAILED;
            }            

            if (ICTCLAS_Init(filePath))
            {
                this.InitStopWordTable();

                return ErrorCode.INIT_SUCCESS;
            }
            else
                return ErrorCode.INIT_FAILED;
        }

        public int Dispose()
        {
            if (ICTCLAS_Exit())
            {
                return ErrorCode.DISPOSE_SUCCESS;
            }
            else
                return ErrorCode.DISPOSE_FAILED;
        }

        public int CostFunction(int Count, float threshold)
        {
            this.threshold = threshold;

            return 0;
        }

        public int DeliveryData(DataTable dataTable)
        {
            Stopwatch sw1 = new Stopwatch();
            sw1.Start();

            List<Sentence> mesgList = null;
            mesgList = dataTable.AsEnumerable().AsParallel().Select(row => DataTable2Sentence(row)).ToList();

            sw1.Stop();
            Console.WriteLine("delivery data took {0}", sw1.Elapsed);

            if (mesgList == null)
            {
                return ErrorCode.DELIVERY_DATA_FAILED;
            }
            else
            {
                this.mesgBlockCollection.Add(mesgList);

                
                return ErrorCode.DELIVERY_DATA_SUCCESS;
            }
        }

        

        public int GetClustered(out DataTable dataTableResult)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            this.mesgBlockCollection.CompleteAdding();

            List<List<Sentence>> dataResult = null;

            try
            {
                Stopwatch sw2 = new Stopwatch();
                sw2.Start();
                List<List<Sentence>> classifiedSentenceCollection = ClassifySentence(this.mesgBlockCollection);
                sw2.Stop();
                Console.WriteLine("classify collection took {0}", sw2.Elapsed);

                Stopwatch sw3 = new Stopwatch();
                sw3.Start();
                dataResult = classifiedSentenceCollection.AsParallel().Select(sList => ComputeCluster(sList)).
                    Aggregate((list1, list2) =>
                    {
                        list1.AddRange(list2);
                        return list1;
                    });
               
                sw3.Stop();
                Console.WriteLine("cluster took {0}", sw3.Elapsed);
            }
            catch (AggregateException ae)
            {
                dataTableResult = null;
                foreach (var e in ae.InnerExceptions)
                {
                    Console.WriteLine("Exceptions: {0}", e.ToString());
                }
            }
            sw.Stop();
            Console.WriteLine("Time took in cluster {0}", sw.Elapsed);
            if (this.resultDataTable == null)
            {
                dataTableResult = null;
                return ErrorCode.GET_RESULT_FAILED;
            }
            else
            {
                Stopwatch sw4 = new Stopwatch();
                sw4.Start();
                this.resultDataTable.Columns.Add(new DataColumn("ClusterName", typeof(string)));
                this.resultDataTable.Columns.Add(new DataColumn("idSet", typeof(string)));
                this.resultDataTable.Columns.Add(new DataColumn("count", typeof(int)));
                foreach (List<Sentence> cluster in dataResult)
                {
                    string idSet = "";
                    string clusterName = "";
                    bool flag = true;
                    foreach (Sentence s in cluster)
                    {
                        idSet += s.id.ToString() + ",";

                        if (flag == true)
                        {
                            foreach (string st in s.words)
                            {
                                clusterName += st;
                            }
                            flag = false;
                        }
                    }
                    DataRow row = resultDataTable.NewRow();
                    
                    
                    row["ClusterName"] = clusterName;
                    row["idSet"] = idSet;
                    row["count"] = cluster.Count;
                    resultDataTable.Rows.Add(row);
                }

                dataTableResult = this.resultDataTable;
                sw4.Stop();
                Console.WriteLine("make result datatable took {0}", sw4.Elapsed);

                return ErrorCode.GET_RESULT_SUCCESS;
            }
            
        }

        public int AbortAll()
        {
            return ErrorCode.ABORTALL_SUCCESS;
        }

       
        private HashSet<string> SplitWord(string input, out System.Int32 hashcode)
        {
            HashSet<string> words = null;
            // do word splitation here.
            var splitedResult = new result_t[input.Length];
            int wordCount = ICTCLAS_ParagraphProcessAW(input, splitedResult, eCodeType.CODE_TYPE_UNKNOWN, 1);
            byte[] inputByte = System.Text.Encoding.Default.GetBytes(input);
            words = GetSplitedWords(inputByte, splitedResult, wordCount, out hashcode);

            return words;
        }

        private HashSet<string> GetSplitedWords(byte[] inputBytes, result_t[] splitedResult, int wordCount, out System.Int32 hashcode)
        {
            HashSet<string> wordList = new HashSet<string>();
            string wordsSense = "";
            for (int i = 0; i < wordCount; i++)
            {
                byte[] wordBytes = new byte[splitedResult[i].length];
                Array.Copy(inputBytes, splitedResult[i].start, wordBytes, 0, wordBytes.Length);
                string word = System.Text.Encoding.Default.GetString(wordBytes);
                if (string.IsNullOrWhiteSpace(word) || this.stopWrodHashTable.Contains(word))
                {
                    continue;
                }
                wordsSense += word;
                wordList.Add(word);
            }
            hashcode = wordsSense.GetHashCode();
            return wordList;
        }

        /// <summary>
        /// do classify
        /// shuffle
        /// </summary>
        /// <param name="sentenceList">input sentences</param>
        /// <returns>classified sentences</returns>
        protected virtual List<List<Sentence>> ClassifySentence(BlockingCollection<List<Sentence>> bc)
        {
            Stopwatch sw5 = new Stopwatch();
            sw5.Start();
            List<Sentence> unqueList;
            unqueList = bc.AsParallel().Select(sList => GetUnque(sList)).
                Aggregate((list1, list2) =>
                {
                    list1.AddRange(list2);
                    return list1;
                });
            sw5.Stop();
            Console.WriteLine("unque took {0}", sw5.Elapsed);
           
            var ret = new List<List<Sentence>>();
            
            List<Sentence> sentencesLess4 = new List<Sentence>();
            List<Sentence> sentencesOver4 = new List<Sentence>();

            sentencesLess4.AddRange(unqueList.Where(s => s.words.Count <= 4).ToList());
            sentencesOver4.AddRange(unqueList.Where(s => s.words.Count > 4).ToList());

            ret.Add(sentencesOver4);
            ret.Add(sentencesLess4);
            Console.WriteLine("---{0}, {1}", sentencesLess4.Count, sentencesOver4.Count);

            return ret;
        }

        private List<Sentence> GetUnque(List<Sentence> sentList)
        {
            List<Sentence> list1 = new List<Sentence>();
            Hashtable table = new Hashtable();
            foreach (Sentence sent in sentList)
            {
                if (!table.ContainsKey(sent.hashCode))
                {
                    table.Add(sent.hashCode, 1);
                    list1.Add(sent);
                }
            }
            table.Clear();

            return list1;
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
                    if (curRef.words.Count < this.threshold * sentenceList[i].words.Count)
                    {
                        left.AddRange(sentenceList.GetRange(i, (sentenceList.Count - i)));
                        break;
                    }
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
        }

        private int LoadConfigFile(string filePath)
        {
            try
            {
                XmlDocument xml = new XmlDocument();
                string xmlFile = Path.Combine(filePath, "Configure.xml");

                if (!File.Exists(xmlFile))
                {
                    return -1;
                }

                xml.Load(xmlFile);
                string tmpValue = null;

                if (xml.GetElementsByTagName("dictDir").Count > 0)
                {
                    tmpValue = xml.DocumentElement["dictDir"].InnerText.Trim();
                    this.dictDir = tmpValue;
                }
                if (xml.GetElementsByTagName("maxThreadCount").Count > 0)
                {
                    tmpValue = xml.DocumentElement["maxThreadCount"].InnerText.Trim();
                    this.maxThreadCount = Convert.ToInt32(tmpValue);
                }
                return 0;
            }
            catch (System.Exception ex)
            {
                throw new Exception("Load Configure.xml file error" + ex.Message);
            }
        }

        private void InitStopWordTable()
        {
            // stop words hash table
            Hashtable stopHT = new Hashtable();
            // get stop words file
            string stopwordFile = Path.Combine(filePath, dictDir, "stopwords1.txt");
            //
            try
            {
                StreamReader stopWordSR = new StreamReader(stopwordFile, System.Text.Encoding.Default);
                //
                string word = "";
                //
                word = stopWordSR.ReadLine();
                //
                while (word != null)
                {
                    if (word == "")
                    {
                        stopWordSR.ReadLine();
                        continue;
                    }
                    // add to hash table
                    stopHT.Add(word, 1);
                    //
                    word = stopWordSR.ReadLine();
                }
                //
                stopWordSR.Close();
                //
                this.stopWrodHashTable = stopHT;
            }
            catch (System.Exception swEx)
            {
                throw new Exception("create stopword hashtable exception" + swEx.Message);
            }
        }


        private Sentence DataTable2Sentence(DataRow row)
        {
            Sentence sentence = new Sentence();
            sentence.id = Convert.ToInt32(row["id"]);
            sentence.words = SplitWord(row["content"].ToString(), out sentence.hashCode);

            return sentence;
                
        }




    }


    
    
}
