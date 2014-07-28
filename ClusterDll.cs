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
    }

    public class Center
    {
        public string clusterName;
        public HashSet<string> words;
        public System.Int32 hashcode;
        public string idSet;
        public int Count;
    }

    public class ClusterDll : ClusterInterface
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
        private BlockingCollection<DataTable> dataTableCollection = new BlockingCollection<DataTable>();
        //
        private List<Center> centerList = new List<Center>();
        //
        private Thread splitThread;
        //
        private Thread clusterThread;
        //
        //private bool isAllFinished = false;
        //
        //private bool isDeliveryComplete = false;
        //
        //private bool isSplitComplete = false;
        private AutoResetEvent autoEvent = new AutoResetEvent(false);
        //private ManualResetEvent manualEvent;
        //private BlockingCollection<List<Center>> result = new BlockingCollection<List<Center>>();

        
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
                                
                this.splitThread = new Thread(SpliteThread);
                this.splitThread.Start();
                                
                this.clusterThread = new Thread(ClusterThread);
                this.clusterThread.Start();
                
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
            try
            {
                this.dataTableCollection.TryAdd(dataTable);
            }
            catch (System.Exception ex)
            {
                Console.WriteLine(ex.Message);
                return ErrorCode.DELIVERY_DATA_FAILED;
            }
            return ErrorCode.DELIVERY_DATA_SUCCESS;
        }

            

        public int GetClustered(out DataTable dataTableResult)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            this.dataTableCollection.CompleteAdding();
            //this.isDeliveryComplete = true;

            autoEvent.WaitOne();
            /*while (!this.isAllFinished)
            {
                Thread.Sleep(1000);
            }*/
            //Console.WriteLine("this center list count is {0}", this.centerList.Count);
            if (this.centerList.Count == 0)
            {
                dataTableResult = null;
                return ErrorCode.GET_RESULT_FAILED;
            }
            else
            {
                //Stopwatch sw4 = new Stopwatch();
                //sw4.Start();
                this.resultDataTable.Columns.Add(new DataColumn("ClusterName", typeof(string)));
                this.resultDataTable.Columns.Add(new DataColumn("idSet", typeof(string)));
                this.resultDataTable.Columns.Add(new DataColumn("count", typeof(int)));

                /*Parallel.ForEach(this.centerList, center =>
                {
                    DataRow row = resultDataTable.NewRow();
                    row["ClusterName"] = center.clusterName;
                    row["idSet"] = center.idSet;
                    row["count"] = center.Count;
                    resultDataTable.Rows.Add(row);
                });*/
                Stopwatch s1 = new Stopwatch();
                s1.Start();
                foreach (Center center in this.centerList)
                {
                    DataRow row = resultDataTable.NewRow();
                    row["ClusterName"] = center.clusterName;
                    row["idSet"] = center.idSet;
                    row["count"] = center.Count;
                    resultDataTable.Rows.Add(row);
                }
                s1.Stop();
                Console.WriteLine("make datatable result take {0}", s1.Elapsed);


                dataTableResult = this.resultDataTable;
                //sw4.Stop();
                //Console.WriteLine("make result datatable took {0}", sw4.Elapsed);

                return ErrorCode.GET_RESULT_SUCCESS;
            }
                    
                       
        }

        public int AbortAll()
        {
            return ErrorCode.ABORTALL_SUCCESS;
        }

        private void SpliteThread()
        {
            Stopwatch s2 = new Stopwatch();
            s2.Start();
            while (!this.dataTableCollection.IsCompleted)
            {
                try
                {
                    DataTable data = null;
                    this.dataTableCollection.TryTake(out data);
                    if (data != null)
                    {
                        List<Sentence> mesgList = null;
                        mesgList = data.AsEnumerable().AsParallel().Select(row => DataTable2Sentence(row)).ToList();
                        if (mesgList != null)
                        {
                            this.mesgBlockCollection.Add(mesgList);
                            /*if (this.isDeliveryComplete && this.dataTableCollection.IsCompleted)
                            {
                                this.isSplitComplete = true;
                                this.mesgBlockCollection.CompleteAdding();
                            }*/
                        }
                    }
                }
                catch (InvalidOperationException)
                {
                }

            }
            this.mesgBlockCollection.CompleteAdding();
            //this.isSplitComplete = true;
            //Console.WriteLine("Split words complete!");
            s2.Stop();
            Console.WriteLine("split thread take {0}", s2.Elapsed);
        }

        private void ClusterThread()
        {
            Stopwatch s3 = new Stopwatch();
            s3.Start();
            while (!this.mesgBlockCollection.IsCompleted)
            {                
                try
                {
                    List<Sentence> sentenceList = null;
                    this.mesgBlockCollection.TryTake(out sentenceList);
                    
                    if (sentenceList != null)
                    {
                        //Console.WriteLine("message collection take {0} and will enter ClusterProcess", i);
                        //i++;
                        ClusterProcess(sentenceList);
                    }
                }
                catch (InvalidOperationException)
                {
                }

            }
            //this.result.Add(this.centerList);
            autoEvent.Set();
            //if (this.isSplitComplete && this.mesgBlockCollection.IsCompleted)
            //{
                //this.isAllFinished = true;
                //Console.WriteLine("is all finished");
            //}
            //Console.WriteLine("cluster thread end!");
            s3.Stop();
            Console.WriteLine("cluster thread take {0}", s3.Elapsed);
            
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

        private void ClusterProcess(List<Sentence> sentenceList)
        {
            /*this.centerList = sentenceList.AsParallel().Select(sent => ComputeCluster(sent, this.centerList)).
                Aggregate((list1, list2) =>
                {
                    list1.AddRange(list2);
                    return list1;
                });*/
            //Console.WriteLine("sentence list length is {0}", sentenceList.Count);
            Stopwatch s4 = new Stopwatch();
            s4.Start();
            foreach (Sentence s in sentenceList)
            {
                this.centerList = ComputeCluster(s, this.centerList);
            }
            s4.Stop();
            Console.WriteLine("the core process take {0}", s4.Elapsed);
        }

        private List<Center> ComputeCluster(Sentence sentence, List<Center> centerList)
        {
            if (centerList.Count == 0)
            {
                
                centerList.Add(Sentence2Center(sentence));

                return centerList;
            }
            else
            {
                centerList = centerList.OrderBy(c => c.words.Count).ToList();
                int iCur = 0;
                bool isSimi = false;
                while ( (iCur < centerList.Count)
                    && (sentence.words.Count <= (sentence.words.Count / this.threshold)) )
                {
                    centerList[iCur] = UpdateCenter(sentence, centerList[iCur], out isSimi);
                    iCur++;
                    if (isSimi)
                    {
                        break;
                    }
                }
                if (!isSimi)
                {
                    centerList.Add(Sentence2Center(sentence));
                }

                return centerList;
            }            
        }

        private Center Sentence2Center(Sentence sentence)
        {
            Center center = new Center();
            string name = "";
            foreach (string s in sentence.words)
            {
                name += s;
            }
            center.clusterName = name;
            center.hashcode = sentence.hashCode;
            center.words = sentence.words;
            string idSet = sentence.id.ToString() + ",";
            center.idSet = idSet;
            center.Count = 1;

            return center;
        }

        private Center UpdateCenter(Sentence s, Center c, out bool isSimilar)
        {
            if (s.hashCode == c.hashcode)
            {
                c.idSet += s.id.ToString() + ",";
                c.Count += 1;
                isSimilar = true;
                return c;
            }
            else
            {
                if (jaccardCoefficient(c.words, s.words) >= this.threshold)
                {
                    c.idSet += s.id.ToString() + ",";
                    c.Count += 1;
                    isSimilar = true;
                }
                else
                {
                    isSimilar = false;
                }

                return c;
            }

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
