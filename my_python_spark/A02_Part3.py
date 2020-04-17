# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark
from datetime import timedelta, datetime

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res
    if (len(params) == 7):
        res = tuple(params)

    # 5. We return res
    return res

def get_date(date_str) -> str:
  date = date_str.split(" ")[0]
  parts = date.split("-")
  day = parts[0]
  month = parts[1]
  year = parts[2]
  
  return "{}-{}-{}".format(year, month, day)

def get_time(date_str) -> str:
  return date_str.split(" ")[1]

def get_key_value(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line char
    line = line.replace('\n', '')

    # 3. We get the key and value
    words = line.split(' ')
    day = words[0]
    hour = words[1]

    # 4. We process the value
    hour = hour.rstrip(')')
    hour = hour.strip('(')

    # 4. We assign res
    res = (day, hour)

    # 5. We return res
    return res
  
def get_num_minutes_ago(date: str, time: str, time_interval: int):
    date_info = date.split("-")
    time_info = time.split(":")

    year = int(date_info[0])
    month = int(date_info[1])
    day = int(date_info[2])

    hour = int(time_info[0])
    minute = int(time_info[1])
    second = int(time_info[2])

    date_time_object = datetime(year, month, day, hour, minute, second)

    time_minutes_ago = date_time_object - timedelta(minutes=time_interval)

    date_string = time_minutes_ago.strftime("%Y-%m-%d")
    time_string = time_minutes_ago.strftime("%H:%M:%S")

    return (date_string, time_string)

def reduce_function(collected_output, measurement_time):
  reduce_list = list()
  continuations = 0

  for line in collected_output:
      line_info = get_key_value(line)
      date = line_info[0]
      time = line_info[1]

      reduce_list += [([date, time, 1])]

      previous_time_interval = get_num_minutes_ago(date, time, measurement_time)
      previous_date = previous_time_interval[0]
      previous_time = previous_time_interval[1]

      if len(reduce_list) > 1:
          previous = reduce_list[len(reduce_list) - 2]

          if previous[0] == previous_date and previous[1] == previous_time:
              continuations += 1
          else:
              reduce_list[len(reduce_list) - continuations - 2] = tuple(
                  [reduce_list[len(reduce_list) - continuations - 2][0],
                   reduce_list[len(reduce_list) - continuations - 2][1],
                   continuations + 1]
              )

              for i in range(continuations):
                  reduce_list.pop(-2)
              continuations = 0
              
      return reduce_list
  
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, station_name, measurement_time):
    dataset = sc.textFile(my_dataset_dir)
    
    mappedRDD = dataset.map(process_line)
    filteredRDD = mappedRDD.filter(lambda item: item[1] == station_name and item[0] == '0' and item[5] == '0')
    timesRDD = filteredRDD.map(lambda item: datetime.datetime.strptime(item[4], "%d-%m-%Y %H:%M:%S").strftime("%Y-%m-%d (%H:%M:00)"))
    output = timesRDD.collect()
    
    reduced_output = reduce_function(output, measurement_time)
    
    for item in reduced_output:
      print(item)

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input parameters as needed
    station_name = "Fitzgerald's Park"
    measurement_time = 5

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/3_Assignment/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, station_name, measurement_time)
