using System;

namespace LSHDBLib.Base
{
    public class Node {
        public String alias;
        String url;
        int port;
        bool enabled = true;
        bool local = false;
        public void disable(){
            this.enabled = false;
        }
        public bool isEnabled(){
            return (enabled==true);
        }
        public bool isLocal(){
            return local;
        }
        public void setLocal(){
            this.local=true;
        }
        public Node(String alias,String url, int port, bool enabled) {
            this.alias = alias;
            this.url = url;
            this.port = port;
            this.enabled = enabled;
        }
    }
}