# Running the script
```python ./copyData.py 18.221.97.61 10 1```
where:
* 18.221.97.61 end-point
* 10 limit of records to process
* 1 batch size to send to cassandra

# Installing dependencies locally

```pip install module-name -t ./python_modules```

# Referencing locall dependencies:
```
import sys
sys.path.insert(0, "./python_modules")
```