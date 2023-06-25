import re
import sys

from math  import log10
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env



class proj1(MRJob):
# (word,year),1
#order inversion:(word,*),1
    def mapper(self, _, line):
        year = line[:4]
        words = line[9:]

        words = re.split(" ", words.lower())

        for word in words:
            if(len(word))!=0:
                yield word + "," + str(year), 1
                yield word + ",*", 1

#(word,year),count
#partition sort the word with ascending order,the year with descending order
    def combiner(self, key, values):
        count=sum(values)
        
        yield (key), count
    
    def reducer_init(self):
        self.marginal = 0

#secondary sort:word",",(year,year_freq),the"," is for key seperator from jobconf        
    def reducer(self, key, count):
        year=[]
        year_f=[]
        w1, w2 = key.split(",", 1)
        if w2 == "*":
            self.marginal=sum(count)
                
        else:
            counts = sum(count)
            year.append(w2)
            year_f.append(counts)


        for i in range(len(year)):
            yield w1+",",(year[i],year_f[i])
        
            
        

            

    def my_reducer_init(self):
        self.N = int(jobconf_from_env('myjob.settings.years'))
        self.beta =float(jobconf_from_env('myjob.settings.beta'))
    
    
#calculate tf_idf,join the year and frequence into a list,to calculate the number of years having word.
    def TF_IDF(self, key, year_count):
        word,_=key.split(",",1)
        num = 0
        year = []
        year_f= []
        for f in year_count:
            year.append(f[0])
            year_f.append(f[1])
            num += 1

        IDF = log10(self.N / num)
        
        for i in range(len(year)):
            tfidf=IDF*year_f[i]
            if tfidf>self.beta:
                yield word,year[i]+','+str(tfidf)
                

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer_init=self.reducer_init,
                reducer=self.reducer
            ),
            MRStep(
                reducer_init=self.my_reducer_init,
                reducer=self.TF_IDF
            )
        ]
    SORT_VALUES = True
            
    JOBCONF = {
        'mapreduce.map.output.key.field.separator':',',
        #'mapreduce.job.reduces':2,
        'mapreduce.partition.keypartitioner.options':'-k1,1',
        'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
        'mapreduce.partition.keycomparator.options':'-k1,1 -k2,2nr'
    }

if __name__ == '__main__':
    proj1.run()
