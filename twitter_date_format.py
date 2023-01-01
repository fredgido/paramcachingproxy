from datetime import datetime

# dtime = tweet['created_at']
dtime = 'Fri Oct 09 10:01:41 +0000 2015'
new_datetime = datetime.strptime(dtime,'%a %b %d %H:%M:%S +0000 %Y')
print(repr(new_datetime))


from datetime import datetime
from dateutil.parser import parse

date = 'Fri May 10 00:44:04 +0000 2019'
dt = parse(date)

print(repr(dt))