import math
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

# ---------------------------------!!! Attention Please!!!------------------------------------
# Please add more details to the comments for each function. Clarifying the input 
# and the output format would be better. It's helpful for tutors to review your code.

# We will test your code with the comand like below:
# python3 project1.py -r hadoop hdfs_input -o hdfs_output --jobconf myjob.settings.years=20 --jobconf myjob.settings.beta=0.5 --jobconf mapreduce.job.reduces=2

# Please make sure that your code can be compiled on Hadoop before submission.
# ---------------------------------!!! Attention Please!!!------------------------------------
class proj1(MRJob):

    # define your own mapreduce functions
    SORT_VALUES = True

    JOBCONF = {
        # add your configurations here
    }

    PARTITIONER = "org.apache.hadoop.mapred.lib.TotalOrderPartitioner" #-r hadoop
    # PARTITIONER = None #-r local

    def my_mapper(self, _, text):
        text = str(text.strip())
        if(len(text) > 0):
            tokens = text.split(',', 2)
            #year,term1 term2 .....
            year = tokens[0][0:4]
            for term in tokens[1].split(' '):
                if len(term) > 0:
                    #output: (term, year)
                    yield term,year


    def my_combiner(self, term, years):
        y_count_dict = {}
        #calculate TF
        for year in years:
            y_count_dict[year] = y_count_dict.get(year,0) + 1
        year_fre = ""
        for year,frequency in y_count_dict.items():
            year_fre = year_fre + year + ";" + str(frequency) + ","
        #out: (term, (year1:frequency1, frequency2,...., yearN:frequency)])
        yield term,year_fre[0:len(year_fre)-1]


    def my_reducer(self, term, year_fres):

        N = int(jobconf_from_env('myjob.settings.years'))
        beta = float(jobconf_from_env('myjob.settings.beta'))

        #calculate frequency of each year for term
        year_count_dict = {}

        for year_fre in year_fres:
            for v in year_fre.split(','):
                year_frequency = v.split(";")
                year = year_frequency[0]
                year_count_dict[year] = year_count_dict.get(year,0) + int(year_frequency[1])

        number_of_year_having_term = len(year_count_dict.keys())
        weights = []
        for year,tf in year_count_dict.items():
            idf = math.log10(N / number_of_year_having_term)
            weight = tf * idf
            #weight should > β
            if(weight > beta):
                weights.append(year+","+str(weight))

        if len(weights) > 0:
            #sort the results first by the terms in alphabetical order and then by the years in descending order
            weights.sort()
            for weight in weights:
                #output: (term, weight)
                yield term,weight
    def steps(self):
        return [
            # you decide the number of steps used
            MRStep(mapper=self.my_mapper,
                   combiner=self.my_combiner,
                   reducer=self.my_reducer
                   )
        ]

if __name__ == '__main__':
    proj1.run()