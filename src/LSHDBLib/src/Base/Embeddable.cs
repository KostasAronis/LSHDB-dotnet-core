using System;

namespace LSHDBLib.Base
{
    public interface Embeddable
    {
        void embed(Object v);
        int getSize();
        Embeddable freshCopy();
    }
}