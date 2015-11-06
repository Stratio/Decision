Siddhi Extensions
*****************

This module include some custom extensions created about Siddhi.
 
DistinctWindow
==============

This custom window allow get distint values of an specific field. You can use this custom window using the following query:

```
from testStream #window.stratio:distinct(c1) select c1, c2,c3 insert into resultStream;    
```




