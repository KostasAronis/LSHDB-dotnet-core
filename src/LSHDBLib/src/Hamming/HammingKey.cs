using System;
using LSHDBLib.Base;
using LSHDBLib.Embeddables;
using MathNet.Numerics.Distributions;

namespace LSHDBLib.Hamming
{
    public class HammingKey : Key
    {
        public int t;
    public int[][] samples;
    String[] tokens;
    
    public HammingKey(String keyFieldName, int k,double delta,int t, bool tokenized,bool performComparisons, Embeddable emb){
        this.keyFieldName = keyFieldName;
        this.k = k;
        this.delta = delta;
        this.t = t;
        this.size = emb.getSize();
        optimizeL();
        this.tokenized = tokenized;
        this.samples = new int[this.L][];
        for(int i = 0; i< this.L;i++){
            this.samples[i] = new int[this.k];
        }
        initSamples();
        this.performComparisons = performComparisons;
        this.emb=emb;
        Console.WriteLine("Number of hash tables generated L="+this.L+" using k="+this.k+" and size="+this.size);
    }
    
    public HammingKey(String keyFieldName) :
      this(keyFieldName,30,.1,75,true,true, new BloomFilter(700,15,2))
    {
        
    }
    
    public HammingKey(int k, double delta, int t, int size) {
        this.k = k;
        this.delta = delta;    
        this.t = t;        
        this.size = size;
        optimizeL();  
    }
    public override int optimizeL() {
        L = (int) Math.Ceiling(Math.Log(delta) / Math.Log(1 - Math.Pow((1.0 - (t * 1.0 / this.size)), k)));
        return L;
    }
    public override Key create(double thresholdRatio){
        if (thresholdRatio == 1.0)
            thresholdRatio = 1.23;
        int t = (int) Math.Round(this.t * thresholdRatio);
        return new HammingKey(k,delta,t,emb.getSize());
    }
    
    public int getLc() {
        double p = 1 - (t * 1.0) / (this.size * 1.0);
        p = Math.Pow(p, k);
        double exp = (L * p);
        double std = Math.Sqrt(exp * (1 - p));
        int C=(int) Math.Round(exp-std); 
        
        double x = (Math.Sqrt(Math.Log(delta) * Math.Log(delta) - 2 * C * Math.Log(delta)) - Math.Log(delta) + C) / p;
        int Lc = (int) Math.Ceiling(x);
        double b=Lc*p;
        if (C > b) {
            Console.Error.WriteLine("does not apply C > np.");
        }
        Binomial bd1 = new Binomial(p, L);
        for (int l=L;l<L*2;l++){
           bd1 = new Binomial(p, l);
           double result = bd1.CumulativeDistribution(C-1);
           if (result < delta){
               Lc=l;
               break;
           }
        }
        Console.WriteLine("Lc reduced to="+Lc);
        return Lc;
    }
    
     public void initSamples() {
        Random r = new Random();
        for (int j = 0; j < this.L; j++) {
            for (int k = 0; k < this.k; k++) {
                samples[j][k] = r.Next(this.size);
            }
        }
    }
    }
}