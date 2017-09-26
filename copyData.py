import sys
from datetime import datetime
import time
import logging
sys.path.insert(0, "./python_modules")
###############################################################################
# Utils
###############################################################################
def readFile(path):
  with open(path, 'r') as inputFile:
      data = inputFile.read()
      return data

def formatCqlDateTime(input):
  return input.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

def formatField(input, valueQualifier):
  if input is None:
    return 'null'
  else:
    return valueQualifier + str(input) + valueQualifier
###############################################################################

###############################################################################
# Cassandra Wrapper
###############################################################################
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

def connect(endpoints):
  global _cluster 
  _cluster = Cluster(endpoints)
  global _session 
  _session = _cluster.connect()
  
def executeQuery(cql, params):
  return _session.execute(cql, params)

def openQuery(cql, callback):
#   print(type(_cluster.metadata.keyspaces['perf8rst'].tables['taxtype_31']))
  statement = SimpleStatement(cql, fetch_size=100)
  for dataRow in _session.execute(statement):
      if (linesProcessed <= limit):
        callback(dataRow)
      else:
        break
###############################################################################

startTime = time.time()
cqlTemplate = readFile('./data/taxtypes.cql')
cqlBuffer = []
linesProcessed = 0

endpoints = [sys.argv[1]] if (len(sys.argv) >= 2)  else ['18.221.97.61']
limit = int(sys.argv[2]) if (len(sys.argv) >= 3)  else 1000
cqlBatchSize = int(sys.argv[3]) if (len(sys.argv) >= 4) else 100

def flushData():
  global cqlBuffer
  if len(cqlBuffer) > 0:
    cql = 'begin batch\n' + ''.join(cqlBuffer) + 'apply batch;'
#     print cql
    logging.info('Sending data to c*, batch size: ' + str(len(cqlBuffer)) + ' ...')
    executeQuery(cql, [])
    cqlBuffer = []

def writeData(row):
  global cqlBuffer
  global cqlTemplate
  createdDate = formatCqlDateTime(row.createddate)
  cql = cqlTemplate.format(
    	marketname=formatField(row.marketname, "'"),
      posnumber=formatField(row.posnumber, "'"),
      seqnumber=formatField(row.seqnumber, "'"),
      id=formatField(row.id, ""),
      basis=formatField(row.basis, "'"),
      breakpointtable=formatField(row.breakpointtable, "'"),
      createddate=formatField(createdDate, "'"),
      calcbase=formatField(row.calcbase, "'"),
      grouping=formatField(row.grouping, "'"),
      precision=formatField(row.precision, ""),
      rate=formatField(row.rate, ""),
      rounding=formatField(row.rounding, "'")
  )
  cqlBuffer.append(cql)
  if len(cqlBuffer) >= cqlBatchSize:
    flushData()      

def printStatus():
  global linesProcessed
  logging.info('Rows processed: ' + str(linesProcessed))
    
def readData():
    def callback(row):
      global linesProcessed
      linesProcessed += 1
      writeData(row)
      if (linesProcessed % (limit / cqlBatchSize) == 0):
        printStatus()
      
    openQuery('select * from  perf8rst.taxtype_31', callback)

###############################################################################
# Setting up logging to console and to file
###############################################################################
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create console handler and set level to info
handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# create error file handler and set level to info
handler = logging.FileHandler('./process.log',"w", encoding=None, delay="true")
# handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
    
logging.info('Starting data population on ' + formatCqlDateTime(datetime.now()))
logging.info('Limit: ' + str(limit) + ' cql batch size: ' + str(cqlBatchSize))
###############################################################################

connect(endpoints)
readData()
flushData()
printStatus()

logging.info( 'Time taken: ' + str(time.time() - startTime))