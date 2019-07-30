using System;
using System.Runtime.Serialization;

namespace LSHDBLib.Base
{
    [Serializable]
    internal class StoreInitException : Exception
    {
        private Exception ex;

        public StoreInitException()
        {
        }

        public StoreInitException(Exception ex)
        {
            this.ex = ex;
        }

        public StoreInitException(string message) : base(message)
        {
        }

        public StoreInitException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected StoreInitException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}