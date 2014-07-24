using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace ClusterLibrary
{
    public class ErrorCode
    {
        public const int INIT_SUCCESS = 1;
        public const int INIT_FAILED = 2;
        public const int INIT_LOAD_CONFIG_FAILED = 3;
        public const int DELIVERY_DATA_SUCCESS = 4;
        public const int DELIVERY_DATA_FAILED = 5;
        public const int GET_RESULT_SUCCESS = 6;
        public const int GET_RESULT_FAILED = 7;
        public const int DISPOSE_SUCCESS = 8;
        public const int DISPOSE_FAILED = 9;
        public const int ABORTALL_SUCCESS = 10;
        public const int ABORTALL_FAILED = 11;
    }
   
    public interface ClusterInterface
    {
        

        int Init(string configFilePath);

        int CostFunction(int Count, float threshold);

        int DeliveryData(DataTable dataTable);

        int GetClustered(out DataTable dataTableResult);

        int AbortAll();

        int Dispose();
    }
}
