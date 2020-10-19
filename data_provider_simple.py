from datetime import timedelta, datetime
import sys, time, random, requests
from time import sleep


write_url ="http://localhost:8086/write?db=udf_db&rp=autogen&precision=s"
measurement = "sample_data"
tag1 = 'tag1'
tag2 = 'tag2'

def main():
    line_protocol="{measurement},tag1={tag1},tag2={tag2} field1={field1},field2={field2} {date}"
    now = datetime(2020, 9, 28)
    epoch = datetime(1970, 1, 1)
    second = timedelta(seconds=1)

    time_s = 1601949655
    epoch_seconds = time_s

    for i in range(20):
        #epoch_seconds = (now - epoch).total_seconds()
        epoch_seconds += 10
        data = line_protocol.format(measurement=measurement, tag1=tag1, tag2= tag2, field1=random.randrange(1, 5), 
            field2=random.randrange(1,5), date=int(epoch_seconds))
        now += second

        print(data)
        r = requests.post(write_url, data=data)
        if r.status_code != 204:
            print(r.status_code)
            print("Error while writing data")
           
        sleep(1) 

if __name__ == "__main__":
    main()
    

