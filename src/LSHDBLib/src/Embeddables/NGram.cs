using System;
using System.Collections.Generic;

namespace LSHDBLib.Embeddables
{
    public class NGram {

        public static List<String> getGrams(String word, int n) {
            List<String> ngrams = new List<String>();
            int len = word.Length;
            for (int i = 0; i < len; i++) {
                if (i > (n - 2)) {
                    String ng = "";
                    for (int j = n - 1; j >= 0; j--) {
                        ng = ng + word[i - j];
                    }
                    ngrams.Add(ng);
                }
            }
            return ngrams;
        }
    }
}