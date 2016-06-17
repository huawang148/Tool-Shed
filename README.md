# hive-python-streaming
Instead of Java UDF, Python can be used to Hadoop/Hive streaming data

Hive is a popular data warehouse built on top on hadoop that supports SQL like query instead of writing mapreduce jobs. Although its SQL like language, called HQL makes hadoop more friendly to use for analysts and data scientists, SQL has language as a whole has many limitations, especially for more complex data transformation and analysis. Just like other SQL based databases, Hive supports adding User define functions and is mostly Java based. I found UDF is useful and convenient for solving some function like problems; for example, implementation Havensine distance function given 2 pairs of lat lons; so function can be directly called instead of writing crazy SQL everytime. 

However, not all analysts and data scientists are good with Java, and UDF can be too costly to develop for a problem that’s unique enough and not occur frequently. In thise case, I found Python is a good candidate to use with the Hadoop/Hive streaming capability. It basically streams HDFS data line by line as string, and applies Python code to transform the data to get the desire output.


we will give an example of how to write Python streaming script to work with hive. 

First, we will import some handy python modules, and define a couple functions to manipulate the input data.  
 
Remember, data is passed as string and read line by line into python, so we need to "clean up" the input and convert into Python data types.  
   
In the following example, I am reading hive complex types (array of structs), so remove {} and [] to make all columns a single string.    

{% highlight python %}
import sys
from operator import itemgetter
import pytz
from datetime import datetime

def stripLine(line):
    """strip and remove brackets,braces"""
    line = line.strip()
    line = str(line).replace("[", '').replace(']', '').replace('{', '').replace('},', ';').replace('}', '').replace('"','')
    return line
{% endhighlight %}

Then add additional functions you need for your analysis.   

We can take eaily take advantage of the rich modules in Python that hive lacks. 

{% highlight python %}
def buildListOfDict (some_string):
    """Take string that was array of structs in Hive and build into list of dict in python"""
    arr = []
    some_list = some_string.split(';')
    for i in range(len(some_list)):
        arr.append(dict(item.split(":") for item in some_list[i].split(",")))
    return arr

def convert_to_date (timestamp_ms):
    """Take a string of UTC timestamp and convert into date in new york timezone"""
    ts = long(timestamp_ms)/1e3
    utc_tz = pytz.timezone('UTC')
    utc_dt = utc_tz.localize(datetime.utcfromtimestamp(ts))
    ny_tz = pytz.timezone('America/New_York')
    ny_dt = ny_tz.normalize(utc_dt.astimezone(ny_tz)).strftime('%Y-%m-%d %H:%M:%S')
    return ny_dt
{% endhighlight %}

Finally, we run the stdin for each line to transform the data and return the output back as string

{% highlight python %}
for line in sys.stdin:
    strip_line = stripLine(line)

    #provide input fields here - in the actual order
    ip,imp,pf,click = strip_line.split('\t')

    #transform hive complex fields into python list of dicts
    imp_dict_list = buildListOfDict(imp)
    sorted_imp = sorted(imp_dict_list, key=itemgetter('ts'))
    pf_dict_list = buildListOfDict(pf)
    sorted_pf = sorted(pf_dict_list, key=itemgetter('ts'), reverse=True)

    if last_pf_ts > first_imp_ts: #conversions can be attributed to earlier impression(s)
                for p in sorted_pf:
                    path = []
                    for i in sorted_imp:
                        if long(p['ts']) > long(i['ts']):
                            path.append(i)
                    if len(path) > 0:
                        path.append(p)
                        output = generate_output(path, conversion_type)
                        print '\t'.join(output)
                    else:
                        del path

{% endhighlight %}

If packages like NumPy, Pandas are installed, sophisticated data mining like affinity analysis, segmentation, 
recommendation engine can be run, that would be otherwise difficult (if not impossible) to do in Hive with HQL.

See here for the whole example [script](https://github.com/MiningBee/hive-python-streaming/blob/master/hive_python_transformation.py).